package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "lab2/consumidores/proto"
)

type Consumidor struct {
	pb.UnimplementedCyberDayServiceServer
	id         			string
	direccion        	string
	categorias 			[]string
	tiendas   			[]string
	precioMax  			int32
	ofertasRecibidas 	[]*pb.OfertaRequest
	archivoCSV     		string
	ofertasCount    	int
	mu               	sync.Mutex
	probabilidadFallo 	float64
	enFallo          	bool
	caidasSimuladas  	int
	client            	pb.CyberDayServiceClient
}

func cargarConfiguracion(archivo string, numeroCliente int) (*Consumidor, error) {
	file, err := os.Open(archivo)
	if err != nil {
		return nil, fmt.Errorf("no se pudo abrir el archivo %s: %v", archivo, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error leyendo CSV: %v", err)
	}

	if len(records) < 13 {
		return nil, fmt.Errorf("archivo CSV debe tener al menos 13 filas (header + 12 consumidores)")
	}

	record := records[numeroCliente]

	if len(record) < 4 {
		return nil, fmt.Errorf("línea %d del CSV no tiene suficientes columnas", numeroCliente)
	}

	var categorias []string
	if record[1] != "null" {
		categorias = strings.Split(record[1], ";")
	} else {
		categorias = []string{"null"}
	}

	var tiendas []string
	if record[2] != "null" {
		tiendas = strings.Split(record[2], ";")
	} else {
		tiendas = []string{"null"}
	}

	var precioMax int32
	if record[3] != "null" {
		precioInt, err := strconv.Atoi(record[3])
		if err != nil {
			return nil, fmt.Errorf("precio máximo inválido en línea %d: %s", numeroCliente, record[3])
		}
		precioMax = int32(precioInt)
	} else {
		precioMax = -1 // -1 = sin límite
	}

	puerto := 50060 + numeroCliente
	direccion := os.Getenv("CONSUMIDOR_DIRECCION")
	if direccion == "" {
		direccion = fmt.Sprintf("localhost:%s", puerto)
	}
	archivoCSV := fmt.Sprintf("/output/consumidor_%s.csv", record[0])
	probabilidadFallo := 0.1

	return &Consumidor{
		id:                record[0],
		direccion:         direccion,
		categorias:        categorias,
		tiendas:           tiendas,
		precioMax:         precioMax,
		ofertasRecibidas:  make([]*pb.OfertaRequest, 0),
		archivoCSV:        archivoCSV,
		ofertasCount:      0,
		probabilidadFallo: probabilidadFallo,
		enFallo:           false,
		caidasSimuladas:   0,
	}, nil
}

func (c *Consumidor) registrarEnBroker() {

	resp, err := c.client.RegistrarConsumidor(context.Background(), &pb.RegistroConsumidorRequest{
		ConsumidorId: c.id,
		Categorias:   c.categorias,
		Tiendas:      c.tiendas,
		PrecioMax:    c.precioMax,
		Direccion:    c.direccion,
	})
	if err != nil {
		log.Printf("Error registrando %s: %v", c.id, err)
		return
	}

	if resp.GetExito() {
		log.Printf("%s registrado en broker", c.id)
	} else {
		log.Printf("Registro de %s falló", c.id)
	}
}

func (c *Consumidor) EnviarOferta(ctx context.Context, req *pb.OfertaRequest) (*pb.OfertaResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.enFallo {
		log.Printf("%s en fallo - rechazando oferta", c.id)
		return &pb.OfertaResponse{Exito: false}, nil
	}

	if !c.enFallo && c.probabilidadFallo > 0 {
		if rand.Float64() < c.probabilidadFallo {
			c.simularFallo()
			return &pb.OfertaResponse{Exito: false}, nil
		}
	}
	
	c.ofertasRecibidas = append(c.ofertasRecibidas, req)
	c.ofertasCount++
	
	err := c.escribirEnCSV(req)
	if err != nil {
		log.Printf("Error escribiendo CSV para %s: %v", c.id, err)
		return &pb.OfertaResponse{Exito: false}, nil
	}
	
	log.Printf("%s recibió oferta: %s - $%d", c.id, req.GetProducto(), req.GetPrecio())
	log.Printf("   - Total recibidas: %d ofertas", c.ofertasCount)
	
	return &pb.OfertaResponse{Exito: true}, nil
}

func (c *Consumidor) simularFallo() {
	c.enFallo = true
	c.caidasSimuladas++
	
	log.Printf("%s CAÍDA SIMULADA - Probabilidad: %.1f%%", 
		c.id, c.probabilidadFallo*100)
	log.Printf("   - Caída #%d - Recuperación en 5 segundos", c.caidasSimuladas)
	
	go c.recuperarAutomaticamente()
}

func (c *Consumidor) recuperarAutomaticamente() {
	log.Printf("%s programado para recuperarse en 5 segundos", c.id)
	time.Sleep(5 * time.Second)
	
	log.Printf("%s iniciando resincronización...", c.id)
	
	exito := c.solicitarResincronizacion()
	
	c.mu.Lock()
	if exito {
		c.enFallo = false
		log.Printf("%s COMPLETAMENTE RECUPERADO Y SINCRONIZADO", c.id)
	} else {
		log.Printf("%s falló resincronización - reintentando en 5s", c.id)
		go func() {
			time.Sleep(5 * time.Second)
			c.recuperarAutomaticamente()
		}()
	}
	c.mu.Unlock()
}

func (c *Consumidor) solicitarResincronizacion() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c.mu.Lock()
	ofertasActuales := c.ofertasRecibidas
	c.mu.Unlock()

	resp, err := c.client.SincronizarEntidad(ctx, &pb.SincronizacionRequest{
		EntidadId:      c.id,
		Tipo:           "consumidor",
		OfertasActuales: ofertasActuales,
	})
	
	if err != nil || !resp.GetExito() {
		log.Printf("%s error en resincronización: %v", c.id, err)
		return false
	}

	c.mu.Lock()
	ofertasAntes := c.ofertasCount
	for _, oferta := range resp.GetOfertasFaltantes() {
		existe := false
		for _, ofertaExistente := range c.ofertasRecibidas {
			if ofertaExistente.GetOfertaId() == oferta.GetOfertaId() {
				existe = true
				break
			}
		}
		if !existe {
			c.ofertasRecibidas = append(c.ofertasRecibidas, oferta)
			c.escribirEnCSV(oferta)
		}
	}
	c.ofertasCount = len(c.ofertasRecibidas)
	c.mu.Unlock()

	ofertasNuevas := c.ofertasCount - ofertasAntes
	log.Printf("%s resincronizado: +%d ofertas (%d → %d total)", 
		c.id, ofertasNuevas, ofertasAntes, c.ofertasCount)
	
	return true
}

func (c *Consumidor) escribirEnCSV(oferta *pb.OfertaRequest) error {
	file, err := os.OpenFile(c.archivoCSV, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return err
    }
    defer file.Close()

    writer := csv.NewWriter(file)
    defer writer.Flush()

    record := []string{
        oferta.GetOfertaId(),
        oferta.GetTienda(),
        oferta.GetCategoria(),
        oferta.GetProducto(),
        strconv.Itoa(int(oferta.GetPrecio())),
        strconv.Itoa(int(oferta.GetStock())),
        oferta.GetFecha(),
    }

    return writer.Write(record)
}

func (c *Consumidor) crearArchivoCSVVacio() error {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    file, err := os.Create(c.archivoCSV)
    if err != nil {
        return fmt.Errorf("no se pudo crear archivo CSV: %v", err)
    }
    defer file.Close()

    writer := csv.NewWriter(file)
    defer writer.Flush()

    header := []string{"oferta_id", "tienda", "categoria", "producto", "precio", "stock", "fecha"}
    err = writer.Write(header)
    if err != nil {
        return fmt.Errorf("no se pudo escribir header en CSV: %v", err)
    }
    
    log.Printf("%s - Archivo CSV creado: %s", c.id, c.archivoCSV)
    return nil
}

func main() {
	var numeroCliente int
	flag.IntVar(&numeroCliente, "cliente", 0, "Número del cliente (1-12)")
	flag.Parse()

	if numeroCliente < 1 || numeroCliente > 12 {
		log.Fatal("Debe especificar un número de cliente válido: --cliente=1-12")
	}

	archivoConfig := "consumidores/consumidores.csv"

	rand.Seed(time.Now().UnixNano())

	consumidor, err := cargarConfiguracion(archivoConfig, numeroCliente)
	if err != nil {
		log.Fatalf("Error cargando configuración para cliente %d: %v", numeroCliente, err)
	}

	brokerHost := os.Getenv("BROKER_HOST")
	if brokerHost == "" {
		brokerHost = "broker"
	}
	conn, err := grpc.Dial(brokerHost + ":50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	
	if err != nil {
		log.Fatalf("No se pudo conectar al broker: %v", err)
	}
	defer conn.Close()

	client := pb.NewCyberDayServiceClient(conn)
	
	consumidor.client = client

	log.Printf("Iniciando consumidor: %s (Cliente %d)", consumidor.id, numeroCliente)
	log.Printf("   - Categorías: %v", consumidor.categorias)
	log.Printf("   - Tiendas: %v", consumidor.tiendas)
	log.Printf("   - Precio Máx: %d", consumidor.precioMax)
	log.Printf("   - Probabilidad de fallo: %.1f%%", consumidor.probabilidadFallo*100)
	log.Printf("   - Archivo CSV: %s", consumidor.archivoCSV)

	err = consumidor.crearArchivoCSVVacio()
    if err != nil {
        log.Printf("   No se pudo crear archivo CSV para %s: %v", consumidor.id, err)
    } else {
        log.Printf("Archivo CSV listo para %s", consumidor.id)
    }
	
	grpcServer := grpc.NewServer()
	pb.RegisterCyberDayServiceServer(grpcServer, consumidor)

	listener, err := net.Listen("tcp", consumidor.direccion)
	if err != nil {
		log.Fatalf("Error al iniciar consumidor %s: %v", consumidor.id, err)
	}
	
	go consumidor.registrarEnBroker()

	log.Printf("Consumidor %s listo en puerto %d", consumidor.id, consumidor.direccion)
	log.Printf("Escuchando ofertas...")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Error en servidor consumidor %s: %v", consumidor.id, err)
	}
}
