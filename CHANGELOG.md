# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial AMQP backend scaffold implementing full OJS Backend interface
- In-memory state store for job metadata, workflows, cron, and workers
- HTTP server with all OJS API endpoints via ojs-go-backend-common handlers
- Project structure following established backend patterns (Makefile, Dockerfile, README)
- RabbitMQ topology design per OJS AMQP Binding Spec

### Planned

- RabbitMQ connection management and reconnection logic
- AMQP message publishing for job enqueue operations
- AMQP consumer for job fetch with visibility timeout
- Dead letter exchange integration for retry/DLQ
- gRPC server support
- Conformance test integration (target: Level 0-4)
