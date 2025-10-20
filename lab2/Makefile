.PHONY: proto build build-all mv1 mv2 mv3 mv4 start-all stop-all clean clean-all logs help

proto:
	mkdir -p broker/proto productores/proto nodos/proto consumidores/proto
	protoc --go_out=./broker --go-grpc_out=./broker proto/cyberday.proto
	protoc --go_out=./productores --go-grpc_out=./productores proto/cyberday.proto
	protoc --go_out=./nodos --go-grpc_out=./nodos proto/cyberday.proto
	protoc --go_out=./consumidores --go-grpc_out=./consumidores proto/cyberday.proto

build: proto
	sudo docker-compose -f docker-compose.mv1.yml build
	sudo docker-compose -f docker-compose.mv2.yml build
	sudo docker-compose -f docker-compose.mv3.yml build
	sudo docker-compose -f docker-compose.mv4.yml build

build-mv1:
	sudo docker-compose -f docker-compose.mv1.yml build

build-mv2:
	sudo docker-compose -f docker-compose.mv2.yml build

build-mv3:
	sudo docker-compose -f docker-compose.mv3.yml build

build-mv4:
	sudo docker-compose -f docker-compose.mv4.yml build

# Ejecución de las 4 máquinas:
mv1: build-mv1
	@echo "Iniciando MV1: Riploy + DB1 + Consumidores 5-8"
	sudo docker-compose -f docker-compose.mv1.yml up

mv2: build-mv2
	@echo "Iniciando MV2: Falabellox + DB2 + Consumidores 9-12"
	sudo docker-compose -f docker-compose.mv2.yml up

mv3: build-mv3
	@echo "Iniciando MV3: Parisio + DB3"
	sudo docker-compose -f docker-compose.mv3.yml up

mv4: build-mv4
	@echo "Iniciando MV4: Broker + Consumidores 1-4"
	sudo docker-compose -f docker-compose.mv4.yml up

# Ejecución sin build:
start-mv1:
	sudo docker-compose -f docker-compose.mv1.yml up

start-mv2:
	sudo docker-compose -f docker-compose.mv2.yml up

start-mv3:
	sudo docker-compose -f docker-compose.mv3.yml up

start-mv4:
	sudo docker-compose -f docker-compose.mv4.yml up

# logs
logs-mv1:
	sudo docker-compose -f docker-compose.mv1.yml logs -f

logs-mv2:
	sudo docker-compose -f docker-compose.mv2.yml logs -f

logs-mv3:
	sudo docker-compose -f docker-compose.mv3.yml logs -f

logs-mv4:
	sudo docker-compose -f docker-compose.mv4.yml logs -f

clean:
	@echo "Limpiando contenedores..."
	sudo docker-compose -f docker-compose.mv1.yml down -v
	sudo docker-compose -f docker-compose.mv2.yml down -v
	sudo docker-compose -f docker-compose.mv3.yml down -v
	sudo docker-compose -f docker-compose.mv4.yml down -v
	@echo "Contenedores limpiados"

clean-all: clean
	@echo "Limpiando imágenes Docker..."
	sudo docker image prune -f
	@echo "Limpieza completa"

attach-broker:
	@echo "Conectando al broker de MV4..."
	@CONTAINER=$$(sudo docker-compose -f docker-compose.mv4.yml ps -q broker 2>/dev/null); \
	if [ -z "$$CONTAINER" ]; then \
		echo "Broker no está ejecutándose"; \
	else \
		sudo docker attach $$CONTAINER; \
	fi



