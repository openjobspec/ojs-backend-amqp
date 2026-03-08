module github.com/openjobspec/ojs-backend-amqp

go 1.24.0

toolchain go1.24.4

require (
	github.com/go-chi/chi/v5 v5.2.5
	github.com/openjobspec/ojs-go-backend-common v0.0.0
	github.com/rabbitmq/amqp091-go v1.10.0
)

require github.com/google/uuid v1.6.0 // indirect

replace github.com/openjobspec/ojs-go-backend-common => ../ojs-go-backend-common

replace github.com/openjobspec/ojs-proto => ../ojs-proto
