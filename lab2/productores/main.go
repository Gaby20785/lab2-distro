package main

import (
	"context"
	"flag"
	"log"
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "lab2/productores/proto"
)

type ProductoCatalogo struct {
	productoID  string
	tienda      string
	categoria   string
	producto    string
	precioBase  int
	stockBase   int
}

type Productor struct {
	nombre    		string
	client    		pb.CyberDayServiceClient
	ctx       		context.Context
	catalogo  		[]*ProductoCatalogo
	ofertasEnviadas int
}

var categoriasValidas = []string{
	"Electrónica", "Moda", "Hogar", "Deportes", "Belleza", "Infantil",
	"Computación", "Electrodomésticos", "Herramientas", "Juguetes", 
	"Automotriz", "Mascotas",
}

func (p *Productor) cargarCatalogo() error {
	archivoCatalogo := "catalogos/" + p.nombre + "_catalogo.csv"
	
	file, err := os.Open(archivoCatalogo)
	if err != nil {
		return fmt.Errorf("no se pudo abrir %s: %v", archivoCatalogo, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("error leyendo CSV: %v", err)
	}

	for i, record := range records {
		if i == 0 {
			continue
		}

		if len(record) >= 6 {
			precioBase, _ := strconv.Atoi(record[4])
			stockBase, _ := strconv.Atoi(record[5])

			categoria := record[2]
			if !esCategoriaValida(categoria) {
				log.Printf("%s: Categoría inválida - %s", p.nombre, categoria)
				continue
			}

			producto := &ProductoCatalogo{
				productoID: record[0],
				tienda:     record[1],
				categoria:  record[2],
				producto:   record[3],
				precioBase: precioBase,
				stockBase:  stockBase,
			}

			p.catalogo = append(p.catalogo, producto)
		}
	}

	return nil
}

func (p *Productor) iniciarGeneracionOfertas() {
	rand.Seed(time.Now().UnixNano())

	log.Printf("%s iniciando generación de ofertas...", p.nombre)

	for {

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		respEstado, err := p.client.ConsultarEstado(ctx, &pb.ConsultarEstadoRequest{})
		cancel()
		
		if err != nil {
			log.Printf("%s error consultando estado del sistema: %v", p.nombre, err)
		} else if !respEstado.GetActivo() {
			log.Printf("%s detectó que el sistema está inactivo - terminando ejecución", p.nombre)
			return
		}

		if len(p.catalogo) == 0 {
			log.Printf("%s: Catálogo vacío", p.nombre)
			time.Sleep(5 * time.Second)
			continue
		}

		producto := p.catalogo[rand.Intn(len(p.catalogo))]
		
		oferta := p.generarOferta(producto)
		
		resp, err := p.client.EnviarOferta(p.ctx, oferta)
		if err != nil {
			log.Printf("%s error enviando oferta: %v", p.nombre, err)
		} else {
			if resp.GetExito() {
				p.ofertasEnviadas++
				log.Printf("%s envió oferta #%d: %s - $%d (Stock: %d)", 
					p.nombre, p.ofertasEnviadas, oferta.GetProducto(), oferta.GetPrecio(), oferta.GetStock())
			} else {
				log.Printf("%s: No se pudo enviar oferta", p.nombre)
			}
		}

		espera := time.Duration(1 + rand.Intn(3)) * time.Second
		time.Sleep(espera)
	}
}


func (p *Productor) registrarEnBroker() {
	resp, err := p.client.RegistrarProductor(p.ctx, &pb.RegistroProductorRequest{
		Nombre: p.nombre,
	})

	if err != nil {
		log.Fatalf("Error en registro: %v", err)
	}

	if resp.GetExito() {
		log.Printf("Tienda %s registrada exitosamente", p.nombre)
	} else {
		log.Printf("El registro de la tienda %s falló", p.nombre)
	}
}

func (p *Productor) esperarInicio() {
	log.Printf("%s esperando que el sistema esté listo...", p.nombre)
	
	for {
		resp, err := p.client.SolicitarInicio(p.ctx, &pb.InicioRequest{})
		
		if err != nil {
			log.Printf("%s error consultando inicio: %v", p.nombre, err)
			time.Sleep(5 * time.Second)
			continue
		}
		
		if resp.GetInicio() {
			log.Printf("%s recibió señal de inicio - Comenzando generación de ofertas", p.nombre)
			return
		}
		
		time.Sleep(5 * time.Second)
	}
}

func esCategoriaValida(categoria string) bool {
	for _, cat := range categoriasValidas {
		if cat == categoria {
			return true
		}
	}
	return false
}

func (p *Productor) generarOferta(producto *ProductoCatalogo) *pb.OfertaRequest {
	descuento := 10 + rand.Intn(41)
	precioConDescuento := producto.precioBase * (100 - descuento) / 100

	variacionStock := -rand.Intn(51) 
	stockOferta := producto.stockBase * (100 + variacionStock) / 100
	if stockOferta < 1 {
		stockOferta = 1
	}

	return &pb.OfertaRequest{
		OfertaId: fmt.Sprintf("%s-%d", p.nombre, p.ofertasEnviadas+1),
		Tienda:   p.nombre,
		Categoria: producto.categoria,
		Producto:  producto.producto,
		Precio:    int32(precioConDescuento),
		Stock:     int32(stockOferta),
		Fecha:     time.Now().Format("2006-01-02 15:04:05"),
	}
}

func main() {
	var tienda string
	flag.StringVar(&tienda, "tienda", "", "Nombre de la tienda (Riploy, Falabellox, Parisio)")
	flag.Parse()

	if tienda == "" {
		log.Fatal("Debe especificar el nombre de la tienda: --tienda=Riploy|Falabellox|Parisio")
	}

	log.Printf("Iniciando productor: %s", tienda)

	brokerHost := os.Getenv("BROKER_HOST")
	if brokerHost == "" {
		brokerHost = "broker"  // nombre del servicio en docker-compose
	}
	conn, err := grpc.Dial(brokerHost + ":50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	
	if err != nil {
		log.Fatalf("No se pudo conectar al broker: %v", err)
	}
	defer conn.Close()

	client := pb.NewCyberDayServiceClient(conn)
	ctx := context.Background()

	productor := &Productor{
		nombre: tienda,
		client: client,
		ctx:    ctx,
		ofertasEnviadas: 0,
	}

	productor.registrarEnBroker()

	err = productor.cargarCatalogo()
	if err != nil {
		log.Fatalf("Error cargando catálogo: %v", err)
	}

	log.Printf("Catálogo cargado: %d productos", len(productor.catalogo))

	productor.esperarInicio()

	productor.iniciarGeneracionOfertas()
}