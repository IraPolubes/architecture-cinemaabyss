
```plantuml
@startuml

!include ../C4_templates/C4_Component.puml

title C4 Container Diagram — Cinema System with Strangler Fig Migration and Kafka MVP

Person(user, "User", "Watches movies, manages profile, subscriptions, ratings and favorites")

System_Ext(paymentSystem, "Payment System", "External system for processing payments")
System_Ext(onlineCinemas, "Online Cinemas", "External content partners or online cinema integrations")
System_Ext(s3Storage, "S3 Storage", "Object storage for media files and static content")

System_Boundary(cinemaSystem, "Cinema System") {

    Container(webClient, "Web / Mobile Client", "Browser / Mobile App / Smart TV", "Client application used by users to access the cinema platform")

    Container(apiGateway, "API Gateway / Strangler Proxy", "Backend Gateway", "Single entry point for client requests. Routes traffic to the monolith, Movies Service, or Events Service. Uses feature flags for gradual migration of /api/movies traffic")

    Container(monolith, "Cinema Monolith", "Go Backend Application", "Legacy main application that handles users, subscriptions, payments, content access and non-migrated functionality")

    Container(moviesService, "Movies Service", "Go Microservice", "Extracted service that manages movie metadata: titles, descriptions, genres and ratings")

    Container(eventsService, "Events Service", "Backend Microservice", "MVP service for Kafka integration. Provides HTTP API for creating User, Payment and Movie events, publishes them to Kafka, consumes them back, and writes processing results to service logs")

    Container(kafkaBroker, "Kafka Broker", "Apache Kafka", "Message broker for MVP event processing: user-events, payment-events and movie-events")

    ContainerDb(monolithDb, "Monolith Database", "PostgreSQL", "Stores legacy application data: users, subscriptions, payments, catalog data and other monolith-owned data")

    ContainerDb(moviesDb, "Movies Database", "PostgreSQL", "Stores movies and movie genres used by the Movies Service")
}

' User request flow
Rel(user, webClient, "Uses", "HTTPS")
Rel(webClient, apiGateway, "Sends API requests to", "HTTPS / JSON")

' Strangler Fig routing
Rel(apiGateway, monolith, "Routes legacy and non-migrated requests to", "HTTP / JSON")
Rel(apiGateway, moviesService, "Routes /api/movies requests to when feature flag is enabled", "HTTP / JSON")
Rel(apiGateway, eventsService, "Routes /api/events requests to", "HTTP / JSON")

' Data storage
Rel(monolith, monolithDb, "Reads from and writes to", "SQL")
Rel(moviesService, moviesDb, "Reads from and writes to", "SQL")

' External systems used by the monolith
Rel(monolith, paymentSystem, "Processes payments through", "HTTPS / API")
Rel(monolith, onlineCinemas, "Integrates with content providers", "HTTPS / API")
Rel(monolith, s3Storage, "Stores and reads media content", "S3 API")

' Kafka MVP flow
Rel(eventsService, kafkaBroker, "Publishes User, Payment and Movie events", "Kafka producer")
Rel(kafkaBroker, eventsService, "Delivers events back to internal consumers for logging", "Kafka consumer")

@enduml
```