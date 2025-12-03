#### Dependency Graph

##### Layer 1: Health Checks (Foundation)

```mermaid
graph LR
    subgraph KAFKA["☁️ Kafka Cluster"]
        KB0["Broker 0<br/>Running"]
        KB1["Broker 1<br/>Running"]
        KC0["Controller 0<br/>Running"]
        KC1["Controller 1<br/>Running"]
        KTA["Topics<br/>Accessible"]
        KB0 --> KTA
    end

    subgraph CONNECT["🔌 Kafka Connect"]
        KCR["Connect<br/>Running"]
        KCAPI["Connect API<br/>Available"]
        PSCE["PG Sink<br/>Exists"]
        PSCR["PG Sink<br/>Running"]
        KB0 & KB1 --> KCR
        KCR --> KCAPI --> PSCE --> PSCR
    end

    subgraph POSTGRES["🗄️ PostgreSQL"]
        PGR["PostgreSQL<br/>Running"]
        PGAC["DB Accepts<br/>Connections"]
        SRTE["Table<br/>Exists"]
        PGR --> PGAC --> SRTE
    end

    subgraph FASTAPI["🚀 FastAPI"]
        FAR["FastAPI<br/>Running"]
        FAHE["Health<br/>Endpoint"]
        FARE["Ready<br/>Endpoint"]
        KB0 --> FAR
        FAR --> FAHE --> FARE
    end

    classDef health fill:#e1f5ff,stroke:#01579b,stroke-width:2px,color:#000
    class KB0,KB1,KC0,KC1,KTA,KCR,KCAPI,PSCE,PSCR,PGR,PGAC,SRTE,FAR,FAHE,FARE health
```

##### Layer 2: Connectivity Tests

```mermaid
graph LR
    subgraph HEALTH["Layer 1 Outputs"]
        H1["kafka_broker_0"]
        H2["kafka_broker_1"]
        H3["controller_0/1"]
        H4["kafka_connect"]
        H5["postgresql"]
        H6["fastapi"]
    end

    subgraph CONN["🔗 Network Tests"]
        direction TB
        C1["Broker ↔ Controller"]
        C2["Broker ↔ Broker"]
        C3["Connect → Kafka"]
        C4["Connect → PostgreSQL"]
        C5["FastAPI → Kafka"]
        C6["FastAPI → PostgreSQL"]
        C7["FastAPI DB Connection"]
        C6 --> C7
    end

    subgraph ENDPOINTS["🎯 K8s Endpoints"]
        E1["Kafka Broker<br/>Endpoints"]
        E2["Kafka Controller<br/>Endpoints"]
        E3["PostgreSQL<br/>Endpoints"]
    end

    H1 --> C1 & C2 & C3 & E1
    H2 --> C2 & E1
    H3 --> C1 & E2
    H4 --> C3 & C4
    H5 --> C4 & E3
    H6 --> C5 & C6

    classDef connectivity fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#000
    classDef endpoints fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#000
    class C1,C2,C3,C4,C5,C6,C7 connectivity
    class E1,E2,E3 endpoints
```

##### Layer 3: Functional Tests (Integration)

```mermaid
graph TB
    subgraph LAYER12["Layers 1 & 2 Outputs"]
        L1["kafka_topics_accessible"]
        L2["postgresql_sink_connector"]
        L3["connectivity_validated"]
        L4["sensor_readings_table"]
        L5["fastapi_ready_endpoint"]
    end

    subgraph BASIC["📝 Basic Functions"]
        F1["Topic<br/>Exists"]
        F2["Topic<br/>Config"]
        F3["Connector<br/>Config"]
        L1 --> F1 --> F2
        L2 --> F3
    end

    subgraph PIPELINE["🔄 Pipeline E2E"]
        P1["Kafka → PostgreSQL<br/>message_to_postgresql"]
        F1 & L2 & L3 & L4 --> P1
    end

    subgraph QUERYAPI["📊 FastAPI Query Endpoints"]
        Q1["List<br/>Sensors"]
        Q2["Get Sensor<br/>Data"]
        Q3["Get Sensor<br/>Stats"]
        L5 & L4 --> Q1
        Q1 --> Q2 & Q3
    end

    subgraph E2E["🎯 Complete Workflow"]
        E1["End-to-End<br/>complete_workflow"]
        P1 & Q1 & Q2 & Q3 --> E1
    end

    classDef functional fill:#f3e5f5,stroke:#4a148c,stroke-width:2px,color:#000
    classDef critical fill:#ffebee,stroke:#b71c1c,stroke-width:4px,color:#000
    class F1,F2,F3,Q1,Q2,Q3 functional
    class P1,E1 critical
```

##### Critical Path Visualization

```mermaid
graph LR
    START([Test Start]) --> KB0[Kafka Broker 0]
    KB0 --> KTA[Topics Accessible]
    KTA --> TE[Topic Exists]

    TE --> M2PG{{"🔴 Pipeline E2E<br/>message_to_postgresql"}}

    PSCR[PG Sink Running] --> M2PG
    CONN[Connectivity OK] -.-> M2PG
    SRTE[Table Exists] --> M2PG

    M2PG --> CW{{"🔴 Complete Workflow<br/>complete_workflow"}}

    FARE[FastAPI Ready] --> LS[List Sensors]
    SRTE --> LS
    LS --> GSD[Get Data] & GSS[Get Stats]

    LS & GSD & GSS --> CW

    CW --> END([✅ All Tests Pass])

    classDef critical fill:#ffebee,stroke:#b71c1c,stroke-width:3px,color:#000
    classDef success fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#000
    class M2PG,CW critical
    class END success
```

**Legende:**
- 🔵 **Health (Layer 1):** Pod Status & Basic Endpoints
- 🟠 **Connectivity (Layer 2):** Network Communication Validation
- 🟣 **Functional (Layer 3):** Integration & Pipeline Tests
- 🔴 **Critical Path:** message_to_postgresql & complete_workflow
- 🟢 **Endpoints:** Kubernetes Service Endpoints
