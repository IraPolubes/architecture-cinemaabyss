
```plantuml
@startuml

!include ../C4_templates/C4_Component.puml

title C4 Container Diagram — Streaming Service System

Person(user, "User", "Watches movies, manages subscription, rates movies and saves favorites")

System_Ext(recommendationSystem, "Recommendation System", "External system that generates personalized movie recommendations")

System_Boundary(streamingService, "Streaming Service System") {

    Container(webFrontend, "Web Frontend", "React SPA", "User interface for browsing movies, managing profile, subscription, ratings and favorites")

    Container(apiGateway, "API Gateway", "Backend Gateway", "Single entry point for frontend requests. Routes requests to backend services")

    Container(userService, "User Service", "Backend Service", "Manages users, authentication, profiles and user-related events")

    Container(catalogService, "Catalog Service", "Backend Service", "Manages movies, catalog metadata, search and prepared recommendations")

    Container(subscriptionService, "Subscription Service", "Backend Service", "Manages subscriptions, payments, plans and payment-related events")

    Container(rankingService, "Ranking Service", "Backend Service", "Manages ratings, favorite movies and ranking-related events")

    Container(kafkaBroker, "Kafka Broker", "Apache Kafka", "Message broker for domain events: UserCreated, MovieCreated, PaymentCreated, RatingCreated and RecommendationUpdated")

    ContainerDb(usersDb, "Users Database", "Relational Database", "Stores users, credentials, profiles and authentication data")

    ContainerDb(catalogDb, "Catalog Database", "Relational / Document Database", "Stores movies, genres, actors, metadata and prepared recommendations")

    ContainerDb(subscriptionsDb, "Subscriptions Database", "Relational Database", "Stores subscriptions, plans, payments and discounts")

    ContainerDb(rankingDb, "Ranking Database", "Relational / Analytical Database", "Stores ratings, favorite movies and user-to-movie scores")
}

' User request flow
Rel(user, webFrontend, "Uses", "HTTPS")
Rel(webFrontend, apiGateway, "Sends requests to", "HTTPS / JSON")

' API Gateway to services
Rel(apiGateway, userService, "Routes user requests to", "HTTPS / JSON")
Rel(apiGateway, catalogService, "Routes catalog requests to", "HTTPS / JSON")
Rel(apiGateway, subscriptionService, "Routes subscription and payment requests to", "HTTPS / JSON")
Rel(apiGateway, rankingService, "Routes rating and favorite requests to", "HTTPS / JSON")

' Services to databases
Rel(userService, usersDb, "Reads from and writes to", "SQL")
Rel(catalogService, catalogDb, "Reads from and writes to", "SQL / NoSQL")
Rel(subscriptionService, subscriptionsDb, "Reads from and writes to", "SQL")
Rel(rankingService, rankingDb, "Reads from and writes to", "SQL / Analytical queries")

' Services publish events to Kafka
Rel(userService, kafkaBroker, "Publishes UserCreated events", "Kafka producer")
Rel(catalogService, kafkaBroker, "Publishes MovieCreated events", "Kafka producer")
Rel(subscriptionService, kafkaBroker, "Publishes PaymentCreated events", "Kafka producer")
Rel(rankingService, kafkaBroker, "Publishes RatingCreated events", "Kafka producer")

' Services consume events from Kafka
Rel(kafkaBroker, userService, "Delivers user events", "Kafka consumer")
Rel(kafkaBroker, catalogService, "Delivers movie and recommendation events", "Kafka consumer")
Rel(kafkaBroker, subscriptionService, "Delivers payment events", "Kafka consumer")
Rel(kafkaBroker, rankingService, "Delivers rating events", "Kafka consumer")

' External recommendation events
Rel(recommendationSystem, kafkaBroker, "Publishes RecommendationUpdated events", "Kafka producer")

@enduml
```