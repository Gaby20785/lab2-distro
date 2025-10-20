package main

import(
	"context"
	"log"
	"net"
	"sync"
	"time"
	"fmt"
	"os"
	"bufio"
	"strings"
	
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "lab2/broker/proto"
)

type Broker struct {
	pb.UnimplementedCyberDayServiceServer
	productores  		map[string]*ProductorInfo
	nodos        		map[string]*NodoInfo
	consumidores 		map[string]*ConsumidorInfo
	mu           		sync.Mutex
	ofertasRecibidas 	int	
	escriturasExitosas 	int 
	escriturasFallidas  int
	inicio 				bool
	sistemaActivo		bool 
}

type ProductorInfo struct {
    nombre 				string
    ofertasEnviadas 	int
    ofertasAceptadas 	int
}

type NodoInfo struct {
	nombre            string
	direccion         string
	estado         	  bool
	cantCaidas   	  int
	client            pb.CyberDayServiceClient 
}

type ConsumidorInfo struct {
	id_consumidor 		string
	categorias    		[]string
	tiendas       		[]string
	precio_max    		int32
	direccion 	  		string
	estado 				bool
	ofertasRecibidas 	int 
    archivoCSV 			string
    cantCaidas 			int
	client            pb.CyberDayServiceClient 
}

const (
	N = 3
	W = 2 
	R = 2 
)

var categoriasValidas = []string{
	"Electrónica", "Moda", "Hogar", "Deportes", "Belleza", "Infantil",
	"Computación", "Electrodomésticos", "Herramientas", "Juguetes", 
	"Automotriz", "Mascotas",
}

var tiendasValidas = []string{
	"Riploy", "Falabellox", "Parisio",
}

var nodosValidos = []string{
	"DB1", "DB2", "DB3",
}

func NewBroker() *Broker {
	return &Broker{
		productores: 		make(map[string]*ProductorInfo),
		nodos:      		make(map[string]*NodoInfo),
		consumidores: 		make(map[string]*ConsumidorInfo),
		ofertasRecibidas: 	0,
		escriturasExitosas: 0,
		escriturasFallidas: 0,
		inicio: 			false,
		sistemaActivo:		true,
	}
}

func (b *Broker) RegistrarProductor(ctx context.Context, req *pb.RegistroProductorRequest) (*pb.RegistroResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	nombre := req.GetNombre()

	if !esValido(nombre, tiendasValidas) {
		log.Printf("Productor no válido: %s", nombre)
		return &pb.RegistroResponse{Exito: false}, nil
	}

	if _, existe := b.productores[nombre]; existe {
		log.Printf("Productor %s ya registrado", nombre)
		return &pb.RegistroResponse{Exito: false}, nil
	}

	b.productores[nombre] = &ProductorInfo{
		nombre: 			nombre,
		ofertasEnviadas: 	0,
		ofertasAceptadas: 	0,
	}

	log.Printf("Productor registrado: %s", nombre)
	b.verificarInicio()
	return &pb.RegistroResponse{Exito: true}, nil
}

func (b *Broker) RegistrarNodo(ctx context.Context, req *pb.RegistroNodoRequest) (*pb.RegistroResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	nodoID := req.GetNombre()

	if !esValido(nodoID, nodosValidos) {
		log.Printf("Nodo %s no válido", nodoID)
		return &pb.RegistroResponse{Exito: false}, nil
	}

	if _, existe := b.nodos[nodoID]; existe {
		log.Printf("Nodo %s ya registrado", nodoID)
		return &pb.RegistroResponse{Exito: false}, nil
	}

	conn, err := grpc.Dial(req.GetDireccion(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("No se pudo conectar a %s: %v", nodoID, err)
		return &pb.RegistroResponse{Exito: false}, nil
	}

	client := pb.NewCyberDayServiceClient(conn)

	b.nodos[nodoID] = &NodoInfo{
		nombre:            nodoID,
		direccion:         req.GetDireccion(),
		estado:            true,
		cantCaidas: 	   0,
		client:            client,
	}

	log.Printf("Nodo %s registrado en %s", nodoID, req.GetDireccion())
	b.verificarInicio()
	return &pb.RegistroResponse{Exito: true}, nil
}

func (b *Broker) RegistrarConsumidor(ctx context.Context, req *pb.RegistroConsumidorRequest) (*pb.RegistroResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	consumidorID := req.GetConsumidorId()

	if _, existe := b.consumidores[consumidorID]; existe {
		log.Printf("Consumidor %s ya registrado", consumidorID)
		return &pb.RegistroResponse{Exito: false}, nil
	}

	conn, err := grpc.Dial(req.GetDireccion(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("No se pudo conectar a consumidor %s: %v", consumidorID, err)
		return &pb.RegistroResponse{Exito: false}, nil
	}

	client := pb.NewCyberDayServiceClient(conn)

	b.consumidores[consumidorID] = &ConsumidorInfo{
		id_consumidor: 		consumidorID,
		categorias:    		req.GetCategorias(),
		tiendas:       		req.GetTiendas(),
		precio_max:    		req.GetPrecioMax(),
		direccion:     		req.GetDireccion(),
		estado:            	true,
		ofertasRecibidas: 	0,
    	archivoCSV: 		fmt.Sprintf("consumidor_%s.csv", consumidorID),
    	cantCaidas: 		0,
		client:          	client,
	}

	log.Printf("Consumidor %s registrado en %s", consumidorID, req.GetDireccion())
	log.Printf("-Categorías: %v", req.GetCategorias())
	log.Printf("-Tiendas: %v", req.GetTiendas())
	log.Printf("-Precio Máx: %d", req.GetPrecioMax())
	b.verificarInicio()
	return &pb.RegistroResponse{Exito: true}, nil
}

func (b *Broker) verificarInicio() {
	registrados := len(b.productores) + len(b.nodos) + len(b.consumidores)
	
	if registrados == 18 && !b.inicio {
		b.inicio = true
		log.Printf("Sistema listo: %d/18 entidades registradas", registrados)
	} else {
		log.Printf("Estado -> Productores: %d, Nodos: %d, Consumidores: %d", 
		len(b.productores), len(b.nodos), len(b.consumidores))
		log.Printf("Sistema en proceso: %d/18 entidades registradas", registrados)
	}
}

func (b *Broker) SolicitarInicio(ctx context.Context, req *pb.InicioRequest) (*pb.InicioResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return &pb.InicioResponse{
		Inicio: b.inicio,
	}, nil
}

func (b *Broker) EnviarOferta(ctx context.Context, req *pb.OfertaRequest) (*pb.OfertaResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	tienda := req.GetTienda()
	prod, existe := b.productores[tienda]

	if !existe {
		return &pb.OfertaResponse{Exito: false}, nil
	}

	prod.ofertasEnviadas++

	categoria := req.GetCategoria()
	if !esValido(categoria, categoriasValidas) {
		log.Printf("Oferta rechazada: La categoría es inválida: %s", categoria)
		return &pb.OfertaResponse{Exito: false}, nil
	}

	prod.ofertasAceptadas++
	b.ofertasRecibidas++

	log.Printf("Oferta #%d recibida", b.ofertasRecibidas)
	log.Printf("-Tienda: %s", tienda)
	log.Printf("-Producto: %s", req.GetProducto())
	log.Printf("-Categoría: %s", req.GetCategoria())
	log.Printf("-Precio: $%d", req.GetPrecio())
	log.Printf("-Stock: %d", req.GetStock())
	log.Printf("-ID: %s", req.GetOfertaId())

	exito := b.almacenarOfertaEnNodos(req)

	if exito {
		b.escriturasExitosas++
		log.Printf("Oferta #%d almacenada exitosamente (W=%d)", b.ofertasRecibidas, W)
		go b.distribuirAConsumidores(req)
		return &pb.OfertaResponse{Exito: true}, nil
	} else {
		b.escriturasFallidas++
		log.Printf("Oferta #%d falló - No se alcanzó quorum W=%d", b.ofertasRecibidas, W)
		go b.distribuirAConsumidores(req)
		return &pb.OfertaResponse{Exito: false}, nil
	}
}

func (b *Broker) almacenarOfertaEnNodos(oferta *pb.OfertaRequest) bool {
	log.Printf("Enviando a %d nodos (necesario W=%d)...", len(b.nodos), W)
	
	confirmaciones := 0
	
	for nodoID, nodoInfo := range b.nodos {
		exito := b.enviarOfertaANodo(nodoInfo, oferta)
		if exito {
			confirmaciones++
			log.Printf("%s confirmó escritura", nodoID)
		} else {
			log.Printf("%s falló escritura", nodoID)
		}
	}

	log.Printf("Confirmaciones finales: %d/%d (W=%d)", confirmaciones, len(b.nodos), W)

	if confirmaciones >= W {
		log.Printf("Quorum W=%d alcanzado", W)
		return true
	} else {
		log.Printf("Quorum W=%d no alcanzado", W)
		return false
	}
}

func (b *Broker) enviarOfertaANodo(nodoInfo *NodoInfo, oferta *pb.OfertaRequest) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := nodoInfo.client.EnviarOferta(ctx, oferta)
	
	if err != nil {
		log.Printf("Error enviando a %s: %v", nodoInfo.nombre, err)
		if nodoInfo.estado{
			nodoInfo.estado = false
			nodoInfo.cantCaidas++
		}
		return false
	}

	if resp.GetExito() {
		if !nodoInfo.estado {
			log.Printf("%s se reconectó", nodoInfo.nombre)
			nodoInfo.estado = true
		}
		return true
	} else {
		log.Printf("%s rechazó oferta", nodoInfo.nombre)
		if nodoInfo.estado {
			nodoInfo.estado = false
			nodoInfo.cantCaidas++
		}
		return false
	}
}

func (b *Broker) distribuirAConsumidores(oferta *pb.OfertaRequest) {
	b.mu.Lock()
	defer b.mu.Unlock()
	consumidoresNotificados := 0
	
	for consumidorID, consumidor := range b.consumidores {
		if b.coincideConPreferencias(oferta, consumidor) {
			exito := b.notificarConsumidor(consumidorID, consumidor, oferta)
			if exito{
				consumidoresNotificados++
				consumidor.ofertasRecibidas++
				log.Printf("%s recibió la notificación", consumidorID)
			} else {
				log.Printf("%s no logró recibir la notificación", consumidorID)
			}
		}
	}
	
	log.Printf("Oferta %s distribuida a %d consumidores", oferta.GetOfertaId(), consumidoresNotificados)
}

func (b *Broker) notificarConsumidor(consumidorID string, consumidorInfo *ConsumidorInfo, oferta *pb.OfertaRequest) bool{
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := consumidorInfo.client.EnviarOferta(ctx, oferta)
	
	if err != nil {
		log.Printf("Error notificando a consumidor %s: %v", consumidorID, err)
		if consumidorInfo.estado {
			consumidorInfo.estado = false
			consumidorInfo.cantCaidas++
		}
		return false
	}

	if resp.GetExito(){
		if !consumidorInfo.estado{
			log.Printf("%s se reconectó", consumidorID)
			consumidorInfo.estado = true
		}
		return true
	} else {
		log.Printf("%s no recibió la Notificación %s", consumidorID, oferta.GetProducto())
		if consumidorInfo.estado {
			consumidorInfo.estado = false
			consumidorInfo.cantCaidas++
		}
		return false
	}
}

func (b *Broker) coincideConPreferencias(oferta *pb.OfertaRequest, consumidor *ConsumidorInfo) bool {
	if len(consumidor.categorias) > 0 && consumidor.categorias[0] != "null" {
		categoriaCoincide := false
		for _, categoria := range consumidor.categorias {
			if categoria == oferta.GetCategoria() {
				categoriaCoincide = true
				break
			}
		}
		if !categoriaCoincide {
			return false
		}
	}
	
	if len(consumidor.tiendas) > 0 && consumidor.tiendas[0] != "null" {
		tiendaCoincide := false
		for _, tienda := range consumidor.tiendas {
			if tienda == oferta.GetTienda() {
				tiendaCoincide = true
				break
			}
		}
		if !tiendaCoincide {
			return false
		}
	}
	
	if consumidor.precio_max > 0 && oferta.GetPrecio() > consumidor.precio_max {
		return false
	}
	
	return true
}

func (b *Broker) SincronizarEntidad(ctx context.Context, req *pb.SincronizacionRequest) (*pb.SincronizacionResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	entidadID := req.GetEntidadId()
	tipo := req.GetTipo()
	ofertasActuales := req.GetOfertasActuales()
	log.Printf("Sincronizando %s: %s", tipo, entidadID)

	historialOfertas := b.obtenerHistorialOfertas()
	if historialOfertas == nil {
		log.Printf("No se pudo sincronizar %s - No se alcanzó quorum R=%d", entidadID, R)
		return &pb.SincronizacionResponse{Exito: false}, nil
	}

	var ofertasFaltantes []*pb.OfertaRequest
	if tipo == "consumidor" {
		consumidor, existe := b.consumidores[entidadID]
		if !existe {
			log.Printf("Consumidor %s no encontrado para sincronización", entidadID)
			return &pb.SincronizacionResponse{Exito: false}, nil
		}
		
		for _, ofertaHistorial := range historialOfertas {
			if b.coincideConPreferencias(ofertaHistorial, consumidor) {
				existeEnActuales := false
				for _, ofertaActual := range ofertasActuales {
					if ofertaActual.GetOfertaId() == ofertaHistorial.GetOfertaId() {
						existeEnActuales = true
						break
					}
				}
				
				if !existeEnActuales {
					ofertasFaltantes = append(ofertasFaltantes, ofertaHistorial)
				}
			}
		}
	} else {
		for _, ofertaHistorial := range historialOfertas {
			existe := false
			for _, ofertaActual := range ofertasActuales {
				if ofertaActual.GetOfertaId() == ofertaHistorial.GetOfertaId() {
					existe = true
					break
				}
			}
			if !existe {
				ofertasFaltantes = append(ofertasFaltantes, ofertaHistorial)
			}
		}
	}
	
	log.Printf("%s sincronizado: %d ofertas faltantes", entidadID, len(ofertasFaltantes))
	
	if tipo == "nodo" {
		if nodo, existe := b.nodos[entidadID]; existe {
			nodo.estado = true
			log.Printf("Nodo %s resincronizado", entidadID)
		}
	}

	if tipo == "consumidor" {
		if consumidor, existe := b.consumidores[entidadID]; existe {
			consumidor.estado = true
			consumidor.ofertasRecibidas += len(ofertasFaltantes)
			log.Printf("Consumidor %s resincronizado", entidadID)
		}
	}
	
	return &pb.SincronizacionResponse{
		OfertasFaltantes: ofertasFaltantes,
		Exito: true,
	}, nil
}

func (b *Broker) obtenerHistorialOfertas() []*pb.OfertaRequest {
    var listasOfertas [][]*pb.OfertaRequest
    var nodosIDs []string
    
    for nodoID, nodoInfo := range b.nodos {
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        resp, err := nodoInfo.client.LeerOfertas(ctx, &pb.LecturaRequest{})
        cancel()
        
        if err != nil {
			if nodoInfo.estado {
				nodoInfo.estado = false
				nodoInfo.cantCaidas++
			}
            log.Printf("Error leyendo %s: %v", nodoID, err)
            continue
        }
        
        if resp.GetExito() {
            listasOfertas = append(listasOfertas, resp.GetOfertas())
            nodosIDs = append(nodosIDs, nodoID)
            log.Printf("Nodo %s: %d ofertas", nodoID, len(resp.GetOfertas()))
        } else {
			if nodoInfo.estado {
				nodoInfo.estado = false
				nodoInfo.cantCaidas++
			}
		}
    }
    
    if len(listasOfertas) < R {
        log.Printf("No se alcanzó quorum R=%d, solo %d nodos respondieron", R, len(listasOfertas))
        return nil
    }
    
    for i := 0; i < len(listasOfertas); i++ {
        for j := i + 1; j < len(listasOfertas); j++ {
            if b.sonListasIdenticas(listasOfertas[i], listasOfertas[j]) {
                log.Printf("Nodos %s y %s tienen ofertas idénticas - Quorum R=2 alcanzado", 
                    nodosIDs[i], nodosIDs[j])
                return listasOfertas[i]
            }
        }
    }
    
    log.Printf("No se encontraron dos nodos con ofertas idénticas")
    return nil
}

func (b *Broker) sonListasIdenticas(lista1, lista2 []*pb.OfertaRequest) bool {
    if len(lista1) != len(lista2) {
        log.Printf("Listas de ofertas tienen diferente longitud: %d vs %d", len(lista1), len(lista2))
        return false
    }
    
    for _, oferta1 := range lista1 {
        encontrada := false
        for _, oferta2 := range lista2 {
            if oferta1.GetOfertaId() == oferta2.GetOfertaId() {
                encontrada = true
                break
            }
        }
        if !encontrada {
            log.Printf("Oferta %s no encontrada en segunda lista", oferta1.GetOfertaId())
            return false
        }
    }
    
    return true
}

func esValido(valor string, listaValidos []string) bool {
	for _, valido := range listaValidos {
		if valido == valor {
			return true
		}
	}
	return false
}

func (b *Broker) generarReporteFinal() {
    b.mu.Lock()
    defer b.mu.Unlock()

    filename := "/output/Reporte.txt"
    file, err := os.Create(filename)
    if err != nil {
        log.Printf("Error creando reporte: %v", err)
        return
    }
    defer file.Close()

    file.WriteString("RESUMEN DE PRODUCTORES:\n")
    
    for nombre, prod := range b.productores {
        file.WriteString(fmt.Sprintf("*%s:\n", nombre))
        file.WriteString(fmt.Sprintf("  - Ofertas enviadas: %d\n", prod.ofertasEnviadas))
        file.WriteString(fmt.Sprintf("  - Ofertas aceptadas: %d\n", prod.ofertasAceptadas))
    }

	file.WriteString("\n")

    file.WriteString("ESTADO DE NODOS DE BASE DE DATOS:\n")
    for nombre, nodo := range b.nodos {
        estado := "ACTIVO"
        if !nodo.estado {
            estado = "CAÍDO"
        }
        file.WriteString(fmt.Sprintf("*NODO %s: %s\n", nombre, estado))
		file.WriteString(fmt.Sprintf("  * Caídas simuladas: %d\n", nodo.cantCaidas))
    }
    file.WriteString("\n")

	file.WriteString("MÉTRICAS DE ESCRITURA:\n")
	file.WriteString(fmt.Sprintf("*Escrituras exitosas: %d\n", b.escriturasExitosas))
	file.WriteString(fmt.Sprintf("*Escrituras fallidas: %d\n", b.escriturasFallidas))

    file.WriteString("NOTIFICACIONES A CONSUMIDORES:\n")
    for id, cons := range b.consumidores {
        file.WriteString(fmt.Sprintf("* %s:\n", id))
		file.WriteString(fmt.Sprintf("  - Preferencias: Categorías%v, Tiendas%v, PrecioMax:%d\n", 
            cons.categorias, cons.tiendas, cons.precio_max))
        file.WriteString(fmt.Sprintf("  - Ofertas recibidas: %d\n", cons.ofertasRecibidas))
        file.WriteString(fmt.Sprintf("  - Archivo %s generado.\n", cons.archivoCSV))
        file.WriteString(fmt.Sprintf("  - Caídas simuladas: %d\n", cons.cantCaidas))
    }
    file.WriteString("\n")

    file.WriteString("FALLOS Y RECUPERACIONES: \n")
	file.WriteString("*Fallo de Nodos: \n")
    for nombre, nodo := range b.nodos {
        estado := "Pudo recuperarse exitosamente de todas las caídas"
        if !nodo.estado {
            estado = "No logró recuperarse de la última caída"
        }
        file.WriteString(fmt.Sprintf("\n- NODO %s: %s\n", nombre, estado))
		file.WriteString(fmt.Sprintf("- Caídas simuladas: %d\n", nodo.cantCaidas))
		cantRecuperaciones := nodo.cantCaidas
		if !nodo.estado{
			cantRecuperaciones--
		}
		file.WriteString(fmt.Sprintf("- Reconexiones y sincronización: %d\n", cantRecuperaciones))
    }
	file.WriteString("\n*Fallo de Consumidores: \n")
	for nombre, cons := range b.consumidores {
		estado := "Pudo recuperarse exitosamente de todas las caídas"
        if !cons.estado {
            estado = "No logró recuperarse de la última caída"
        }
        file.WriteString(fmt.Sprintf("\n- Consumidor %s: %s\n", nombre, estado))
		file.WriteString(fmt.Sprintf("- Caídas simuladas: %d\n", cons.cantCaidas))
		cantRecuperaciones := cons.cantCaidas
		if !cons.estado{
			cantRecuperaciones--
		}
		file.WriteString(fmt.Sprintf("- Reconexiones y sincronización: %d\n", cantRecuperaciones))
    }

	file.WriteString("\n"+`Tanto los nodos como los consumidores se caen por 5 segundos y se vuelven a conectar
automáticamente y piden resincronización. Es posible que no se puedan recuperar si estaban caídos cuando se solicitó
el término de la ejecución.` + "\n")

	conclusion := b.generarConclusion()
    file.WriteString(conclusion)

    log.Printf("Reporte final generado: %s", filename)
}

func (b *Broker) generarConclusion() string {

    var conclusion strings.Builder
    conclusion.WriteString("\n=== CONCLUSIÓN ===\n\n")

    consistenciaEscritura := b.escriturasExitosas == b.ofertasRecibidas

    nodosActivos := 0
    nodosCaidas := 0
    for _, nodo := range b.nodos {
        if nodo.estado {
            nodosActivos++
        }
        nodosCaidas += nodo.cantCaidas
    }

    consumidoresActivos := 0
    consumidoresCaidas := 0
    for _, consumidor := range b.consumidores {
        if consumidor.estado {
            consumidoresActivos++
        }
        consumidoresCaidas += consumidor.cantCaidas
    }

    if nodosCaidas == 0 && consumidoresCaidas == 0 {
        conclusion.WriteString("El sistema se mantuvo completamente estable durante toda la simulación, ")
        conclusion.WriteString("cumpliendo estrictamente con las reglas de replicación (N=3, W=2, R=2). ")
        conclusion.WriteString("Todas las ofertas fueron procesadas y distribuidas correctamente.\n\n")
    } else {
        if nodosActivos == 3 && consumidoresActivos == 12 {
            conclusion.WriteString("El sistema demostró alta tolerancia a fallos durante la simulación. ")
            conclusion.WriteString("A pesar de las caídas temporales, se mantuvo la disponibilidad y consistencia ")
            conclusion.WriteString("de escritura (W=2). La recuperación y resincronización de todos los nodos ")
            conclusion.WriteString("y consumidores se completaron exitosamente.\n\n")
            
            conclusion.WriteString("El sistema gestionó adecuadamente las desconexiones y reconexiones, ")
            conclusion.WriteString("asegurando la entrega completa de todas las ofertas relevantes sin pérdida ")
            conclusion.WriteString("de datos, gracias a la funcionalidad de recuperación de histórico basada ")
            conclusion.WriteString("en lecturas distribuidas consistentes (R=2).\n\n")
        } else {
            conclusion.WriteString("El sistema operó en condiciones degradadas durante la simulación. ")
            conclusion.WriteString(fmt.Sprintf("%d nodo(s) se encontraban inactivos al finalizar, ", 3-nodosActivos))
            conclusion.WriteString("sin lograr recuperarse de su última caída. ")
            conclusion.WriteString(fmt.Sprintf("%d consumidor(es) permanecían desconectados ", 12-consumidoresActivos))
            conclusion.WriteString("al término de la ejecución.\n\n")
            
            conclusion.WriteString("A pesar de estos fallos permanentes, el sistema demostró robustez ")
            conclusion.WriteString("al continuar procesando y distribuyendo ofertas a las entidades activas, ")
            conclusion.WriteString("manteniendo el quórum de escritura requerido (W=2).\n\n")
        }
    }

    if consistenciaEscritura {
        conclusion.WriteString("En cuanto a la consistencia: todas las ofertas recibidas fueron ")
        conclusion.WriteString("almacenadas exitosamente en el sistema distribuido. ")
    } else {
        conclusion.WriteString("En cuanto a la consistencia: se identificaron algunas discrepancias ")
        conclusion.WriteString("entre ofertas recibidas y almacenadas. ")
    }

    conclusion.WriteString(fmt.Sprintf("Se procesaron %d ofertas en total, ", b.ofertasRecibidas))
    conclusion.WriteString(fmt.Sprintf("con %d escrituras exitosas ", b.escriturasExitosas))
    
    if b.escriturasFallidas > 0 {
        conclusion.WriteString(fmt.Sprintf("y %d escrituras fallidas. ", b.escriturasFallidas))
    } else {
        conclusion.WriteString("sin escrituras fallidas. ")
    }

    conclusion.WriteString("\n\nMétricas clave del sistema:\n")
    conclusion.WriteString(fmt.Sprintf("• Nodos activos: %d/%d\n", nodosActivos, len(b.nodos)))
    conclusion.WriteString(fmt.Sprintf("• Consumidores activos: %d/%d\n", consumidoresActivos, len(b.consumidores)))
    conclusion.WriteString(fmt.Sprintf("• Total de ofertas procesadas: %d\n", b.ofertasRecibidas))
    conclusion.WriteString(fmt.Sprintf("• Escrituras exitosas: %d\n", b.escriturasExitosas))
    conclusion.WriteString(fmt.Sprintf("• Escrituras fallidas: %d\n", b.escriturasFallidas))
    conclusion.WriteString(fmt.Sprintf("• Consistencia de escritura mantenida: %v\n", consistenciaEscritura))
    
    if nodosCaidas > 0 {
        conclusion.WriteString(fmt.Sprintf("• Caídas de nodos manejadas: %d\n", nodosCaidas))
    }
    if consumidoresCaidas > 0 {
        conclusion.WriteString(fmt.Sprintf("• Caídas de consumidores manejadas: %d\n", consumidoresCaidas))
    }

    return conclusion.String()
}

func (b *Broker) ConsultarEstado(ctx context.Context, req *pb.ConsultarEstadoRequest) (*pb.ConsultarEstadoResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return &pb.ConsultarEstadoResponse{Activo: b.sistemaActivo}, nil
}

func (b *Broker) iniciarInterfazUsuario() {
    log.Printf("   - Escribe 'reporte' o 'fin' para generar reporte y finalizar")
    
    go func() {
        reader := bufio.NewReader(os.Stdin)
        for {
            fmt.Print("broker> ")
            text, _ := reader.ReadString('\n')
            text = strings.TrimSpace(text)
            text = strings.ToLower(text)
            
            switch text {
            case "reporte", "fin", "exit", "quit":
                log.Printf("Generando reporte final por comando de usuario...")
                b.mu.Lock()
                b.sistemaActivo = false
                b.mu.Unlock()

				log.Printf("Esperando 10 segundos para últimas recuperaciones...")
                time.Sleep(10 * time.Second)

				log.Printf("Generando reporte final...")
				b.generarReporteFinal()
                log.Printf("Sistema finalizado")
                os.Exit(0)              
            case "":
            default:
                log.Printf("Comando no reconocido: '%s'", text)
                log.Printf("Escribe 'reporte' o 'fin' para finalizar con la ejecución")
            }
        }
    }()
}

func main() {
	broker := NewBroker()
	grpcServer := grpc.NewServer()
	pb.RegisterCyberDayServiceServer(grpcServer, broker)

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Error al intentar iniciar en puerto 50051: %v", err)
	}

	log.Printf("Broker iniciado en puerto :50051")
	log.Printf("Esperando registros...")
	
	broker.iniciarInterfazUsuario()

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Error al iniciar servidor: %v", err)
	}
}