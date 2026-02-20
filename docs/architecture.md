# Endymion-AI Platform - 4+1 Architecture Views

## Overview of 4+1 Architecture Framework

The 4+1 architectural view model organizes the description of a software architecture using five concurrent views:
1. **Logical View** - System functionality (components, classes, relationships)
2. **Process View** - Runtime behavior (concurrency, performance, scalability)
3. **Development View** - Software module organization (packages, libraries, layers)
4. **Physical View** - Deployment topology (servers, networks, infrastructure)
5. **Scenarios (Use Cases)** - Tying it all together through user journeys

**Plus:** **Team Execution View** - Division of work and responsibilities

For Endymion-AI, the 4+1 framework anchors every conversation between engineering, data science, and product stakeholders. Each view gives the teams a repeatable way to evaluate design decisions against the overarching goal: reliably translating messy agricultural data into predictive insights that feed the app experience. These decisions remain preliminary and will evolve as discovery deepens, but keeping the views synchronized ensures the platform can adapt without losing sight of how changes impact end users, runtime performance, code maintainability, or the production rollout strategy.

### Overall View
<img width="960" height="540" alt="Arquitectura" src="https://github.com/user-attachments/assets/a9d1dc32-701b-498f-8cae-5690d71c4c3d" />


*All architectural choices captured here represent the current draft direction and will be revisited as discovery, pilot feedback, and cost modeling continue.*

## View 1: Logical View (Component Architecture)

### Purpose
Shows the key abstractions in the system as objects or classes, focusing on the **functional requirements** and how the system delivers value to end users.

This view highlights how the platform stitches together heterogeneous datasets, model pipelines, and delivery mechanisms into a cohesive product. It makes explicit where system boundaries lie, which services own core responsibilities, and how data flows from raw ingestion to decision-ready outputs. The diagram also doubles as a communication artifact for onboarding new contributors who need to understand "who talks to whom" before diving into code, while acknowledging that component assignments may shift as we validate assumptions with SML.

```mermaid
graph TB
    subgraph "Endymion-AI Platform"
        subgraph "Data Layer"
            DI[Data Ingestion<br/>Azure Data Factory<br/>Databricks Auto Loader]
            DL[Data Lake<br/>Bronze/Silver/Gold<br/>Delta Lake]
            FS[Feature Store<br/>Databricks Unity Catalog]
        end
        
        subgraph "Intelligence Layer"
            MP[Marbling Predictor<br/>XGBoost Ensemble]
            WP[Weight Predictor<br/>Prophet + XGBoost]
            CPI[CPI Calculator<br/>Economic Synthesis]
        end
        
        subgraph "Decision Support Layer"
            NBA[Next Best Action<br/>Rules Engine]
            SCH[Harvest Scheduler<br/>Capacity Optimizer]
        end
        
        subgraph "API & Security Layer"
            APIM[Azure API Management<br/>Gateway + Rate Limiting]
            AUTH[EntraID B2C<br/>User/Tenant Repository]
        end
    end
    
    subgraph "Endymion-AI App"
        subgraph "Frontend Layer"
            UI[React SPA<br/>ShadCN Components]
        end
        
        subgraph "Backend Services"
            API[App Backend APIs<br/>Azure Functions]
            DB[App Database<br/>Azure SQL<br/>Users, Prefs, Config]
        end
        
        subgraph "Admin Module"
            ADMIN[Administration<br/>User Mgmt, Tenants,<br/>Permissions, Config]
        end
    end
    
    subgraph "External Data Sources"
        BC[Business Central<br/>ERP]
        MEQ[MEQ Imaging<br/>Snowflake API]
        EV[E+V Carcass<br/>REST API]
        FL[Feedlot Systems<br/>SFTP/CSV]
    end
    
    %% Data Flow
    BC --> DI
    MEQ --> DI
    EV --> DI
    FL --> DI
    
    DI --> DL
    DL --> FS
    FS --> MP
    FS --> WP
    MP --> CPI
    WP --> CPI
    CPI --> NBA
    CPI --> SCH
    
    NBA --> APIM
    SCH --> APIM
    MP --> APIM
    WP --> APIM
    
    APIM --> API
    AUTH --> API
    AUTH --> ADMIN
    
    API --> DB
    ADMIN --> DB
    API --> UI
    
    %% Styling
    classDef platform fill:#E3F2FD,stroke:#1976D2,stroke-width:2px,color:#000
    classDef app fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px,color:#000
    classDef external fill:#FFF3E0,stroke:#F57C00,stroke-width:2px,color:#000
    
    class DI,DL,FS,MP,WP,CPI,NBA,SCH,APIM,AUTH platform
    class UI,API,DB,ADMIN app
    class BC,MEQ,EV,FL external
```

### Key Logical Components

| Component | Responsibility | Technology |
|-----------|----------------|------------|
| **Data Ingestion** | Acquire data from external sources | Azure Data Factory, Databricks Auto Loader |
| **Data Lake** | Store raw, cleaned, and aggregated data | Delta Lake (Bronze/Silver/Gold) |
| **Feature Store** | Maintain versioned, point-in-time features | Databricks Feature Store + Unity Catalog |
| **Predictive Models** | Generate forecasts (marbling, weight, CPI) | MLflow, XGBoost, Prophet |
| **Decision Support** | Translate predictions into recommendations | Python rules engines |
| **API Gateway** | Secure and route API requests | Azure API Management |
| **EntraID** | Authenticate users, manage tenants | Azure AD B2C |
| **App Backend** | Business logic for UI | Azure Functions (Python) |
| **App Database** | Store user preferences, config | Azure SQL Database |
| **Frontend** | User interface | React + ShadCN |

---

## View 2: Process View (Runtime Behavior)

### Purpose
Describes the system's **runtime behavior** including concurrency, processes, threads, and how components communicate at runtime.

Elaborating on runtime behavior clarifies how the platform must scale on peak days, which hand-offs require strong SLAs, and where resilience patterns should be applied. The process view also exposes implicit dependencies, such as the reliance of user-facing responsiveness on feature-store latency, so that performance budgets can be tracked during implementation and observability can be layered where it matters most. All timings and concurrency targets are candidates for refinement as usage patterns become clearer.
```mermaid
sequenceDiagram
    participant User as User<br/>(Ranch Manager)
    participant UI as Endymion-AI App<br/>(React SPA)
    participant EntraID as EntraID B2C<br/>(Auth)
    participant AppAPI as App Backend<br/>(Azure Functions)
    participant AppDB as App Database<br/>(Azure SQL)
    participant APIM as API Gateway<br/>(APIM)
    participant Platform as Platform APIs<br/>(Databricks SQL)
    participant Scheduler as Harvest Scheduler<br/>(Decision Engine)
    participant FeatureStore as Feature Store<br/>(Delta Lake)
    
    %% Authentication Flow
    rect rgb(200, 220, 240)
        Note over User,EntraID: Authentication Process
        User->>UI: 1. Navigate to app
        UI->>EntraID: 2. Redirect to login
        EntraID->>User: 3. Show login form
        User->>EntraID: 4. Enter credentials + MFA
        EntraID->>UI: 5. Return auth token
        UI->>AppAPI: 6. Exchange for app session
        AppAPI->>AppDB: 7. Load user preferences
        AppDB-->>AppAPI: 8. User config
        AppAPI-->>UI: 9. Session + preferences
    end
    
    %% Harvest Scheduler Flow
    rect rgb(255, 240, 200)
        Note over User,FeatureStore: Harvest Scheduler Request
        User->>UI: 10. Open Harvest Scheduler
        UI->>AppAPI: 11. GET /harvest/candidates<br/>(with auth token)
        AppAPI->>APIM: 12. GET /api/scheduler/candidates<br/>(validate token, rate limit)
        APIM->>Scheduler: 13. Fetch CPI-ranked animals
        Scheduler->>FeatureStore: 14. Query latest CPI scores<br/>(point-in-time lookup)
        FeatureStore-->>Scheduler: 15. Animal data + CPI
        Scheduler->>Scheduler: 16. Apply capacity rules<br/>(4★, 5★, Ultra, Export)
        Scheduler-->>APIM: 17. Ranked candidates list
        APIM-->>AppAPI: 18. JSON response
        AppAPI->>AppDB: 19. Log user activity
        AppAPI-->>UI: 20. Display harvest candidates
        UI-->>User: 21. Interactive table rendered
    end
    
    %% User Action Flow
    rect rgb(220, 240, 220)
        Note over User,AppDB: Approve Next Best Action
        User->>UI: 22. Click "Approve" on NBA
        UI->>AppAPI: 23. POST /nba/approve<br/>{action_id, animal_id}
        AppAPI->>APIM: 24. POST /api/nba/approve
        APIM->>Platform: 25. Record approval
        Platform->>FeatureStore: 26. Update animal status
        FeatureStore-->>Platform: 27. Confirmation
        Platform-->>APIM: 28. Success
        APIM-->>AppAPI: 29. 200 OK
        AppAPI->>AppDB: 30. Log approval event
        AppAPI-->>UI: 31. Update UI state
        UI-->>User: 32. Show success message
    end
    
    %% Background Batch Process
    rect rgb(240, 220, 240)
        Note over Platform,FeatureStore: Daily Batch Inference (Scheduled)
        Platform->>Platform: 33. Trigger: 2:00 AM daily
        Platform->>FeatureStore: 34. Load features for active animals
        FeatureStore-->>Platform: 35. Feature vectors
        Platform->>Platform: 36. Run predictions<br/>(Marbling, Weight, CPI)
        Platform->>FeatureStore: 37. Write predictions to Delta
        FeatureStore-->>Platform: 38. Confirmation
        Platform->>Platform: 39. Refresh NBA recommendations
        Platform->>Platform: 40. Update Scheduler capacity alerts
    end
```

### Key Process Characteristics

| Process | Execution Model | Frequency | Concurrency |
|---------|----------------|-----------|-------------|
| **User Authentication** | Synchronous | Per session (30 min timeout) | 100 concurrent users |
| **API Requests** | Synchronous (HTTP REST) | Per user action | 1000 req/min (rate limited) |
| **Batch Inference** | Asynchronous (scheduled) | Daily at 2:00 AM | Single-threaded (20 min duration) |
| **Feature Refresh** | Asynchronous (triggered) | Daily at 1:00 AM | Parallel (16 Spark workers) |
| **Data Ingestion** | Asynchronous (event-driven) | Hourly (MEQ), Daily (others) | Parallel per source |

---

## View 3: Development View (Module Organization)

### Purpose
Describes the system's **static organization** in terms of layers, packages, modules, and their dependencies. This is the developer's view.

The development view spells out how the codebase is partitioned to keep data engineering, machine learning, and app development moving in parallel. By documenting module boundaries, technology stacks, and upstream/downstream relationships, we reduce accidental coupling and provide a roadmap for repository structure, CI/CD responsibilities, and backlog planning. Teams can use this section to identify where to enforce linting, testing, and documentation standards, understanding that module boundaries may be rearranged as implementation lessons emerge.

```mermaid
graph TB
    subgraph "Development Layers - Endymion-AI Platform"
        subgraph "PL1: Data Ingestion Module"
            PL1A[ADF Pipelines<br/>Python/JSON]
            PL1B[Databricks Notebooks<br/>PySpark]
            PL1C[Schema Validators<br/>Python]
        end
        
        subgraph "PL2: Data Processing Module"
            PL2A[Bronze Layer ETL<br/>PySpark]
            PL2B[Silver Layer ETL<br/>PySpark + Delta]
            PL2C[Gold Aggregations<br/>PySpark SQL]
            PL2D[Feature Engineering<br/>Databricks Feature Store API]
        end
        
        subgraph "PL3: ML Module"
            PL3A[Model Training<br/>MLflow + XGBoost]
            PL3B[Model Registry<br/>MLflow APIs]
            PL3C[Batch Inference<br/>Databricks ML]
            PL3D[Model Monitoring<br/>Evidently AI]
        end
        
        subgraph "PL4: Business Logic Module"
            PL4A[CPI Calculator<br/>Python]
            PL4B[NBA Rules Engine<br/>Python]
            PL4C[Scheduler Optimizer<br/>Python + OR-Tools]
        end
        
        subgraph "PL5: API Module"
            PL5A[REST Endpoints<br/>FastAPI]
            PL5B[API Schemas<br/>Pydantic]
            PL5C[Authentication Middleware<br/>OAuth2]
        end
        
        subgraph "PL6: Infrastructure Module"
            PL6A[Terraform Templates<br/>HCL]
            PL6B[CI/CD Pipelines<br/>Azure DevOps YAML]
            PL6C[Monitoring Dashboards<br/>Azure Monitor KQL]
        end
    end
    
    subgraph "Development Layers - Endymion-AI App"
        subgraph "AL1: Presentation Module"
            AL1A[React Components<br/>TSX]
            AL1B[ShadCN UI Library<br/>TypeScript]
            AL1C[State Management<br/>Zustand + React Query]
            AL1D[Routing<br/>React Router]
        end
        
        subgraph "AL2: Backend Services Module"
            AL2A[Azure Functions<br/>Python]
            AL2B[API Clients<br/>Python + Requests]
            AL2C[Business Logic<br/>Python]
        end
        
        subgraph "AL3: Data Access Module"
            AL3A[SQL Repositories<br/>SQLAlchemy]
            AL3B[EntraID Integration<br/>MSAL Python]
            AL3C[Database Migrations<br/>Alembic]
        end
        
        subgraph "AL4: Admin Module"
            AL4A[User Management<br/>React + Python]
            AL4B[Tenant Management<br/>React + Python]
            AL4C[Configuration<br/>React + Python]
        end
    end
    
    subgraph "Shared Libraries"
        SHARED1[Common Utils<br/>Python Package]
        SHARED2[API Contract<br/>OpenAPI Spec]
        SHARED3[Auth Library<br/>JWT Validation]
    end
    
    %% Dependencies
    PL1A --> PL2A
    PL1B --> PL2A
    PL2A --> PL2B
    PL2B --> PL2C
    PL2C --> PL2D
    PL2D --> PL3A
    PL3A --> PL3B
    PL3B --> PL3C
    PL3C --> PL4A
    PL4A --> PL4B
    PL4A --> PL4C
    PL4B --> PL5A
    PL4C --> PL5A
    PL3D --> PL5A
    
    AL1A --> AL2A
    AL1B --> AL1A
    AL1C --> AL1A
    AL2A --> AL2B
    AL2B --> AL3A
    AL2A --> AL3B
    AL3A --> AL3C
    AL4A --> AL3A
    AL4B --> AL3B
    
    PL5A --> SHARED2
    AL2B --> SHARED2
    PL5C --> SHARED3
    AL3B --> SHARED3
    PL4A --> SHARED1
    AL2C --> SHARED1
    
    %% Styling
    classDef platform fill:#E3F2FD,stroke:#1976D2,stroke-width:2px,color:#000
    classDef app fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px,color:#000
    classDef shared fill:#FFF3E0,stroke:#F57C00,stroke-width:2px,color:#000   
    class PL1A,PL1B,PL1C,PL2A,PL2B,PL2C,PL2D,PL3A,PL3B,PL3C,PL3D,PL4A,PL4B,PL4C,PL5A,PL5B,PL5C,PL6A,PL6B,PL6C platform
    class AL1A,AL1B,AL1C,AL1D,AL2A,AL2B,AL2C,AL3A,AL3B,AL3C,AL4A,AL4B,AL4C app
    class SHARED1,SHARED2,SHARED3 shared
```

### Module Dependencies & Ownership

| Module | Primary Language | Key Dependencies | Team Owner |
|--------|-----------------|------------------|------------|
| **Platform - Data Ingestion** | Python, PySpark | Azure SDK, Databricks | Gracchus |
| **Platform - Data Processing** | PySpark, SQL | Delta Lake, Unity Catalog | Gracchus |
| **Platform - ML** | Python | MLflow, XGBoost, Prophet, Scikit-learn | Gracchus |
| **Platform - Business Logic** | Python | NumPy, Pandas, OR-Tools | Gracchus |
| **Platform - API** | Python (FastAPI) | Pydantic, JWT | Gracchus |
| **App - Presentation** | TypeScript, React | ShadCN, Zustand, React Query | Romania |
| **App - Backend Services** | Python | FastAPI, Requests | Romania |
| **App - Data Access** | Python | SQLAlchemy, MSAL, Alembic | Romania |
| **App - Admin** | TypeScript, Python | React, SQLAlchemy | Romania |
| **Shared Libraries** | Python, OpenAPI | JWT, JSON Schema | Joint (defined Month 2) |

---

## View 4: Physical View (Deployment Architecture)

### Purpose
Shows how the software is **deployed** on hardware/cloud infrastructure, including servers, networks, storage, and physical topology.

This view clarifies the shared responsibility model between cloud services, highlights the network controls safeguarding PII and operational data, and frames the trade-offs between cost and elasticity. It guides infrastructure-as-code definitions, environment parity across dev/stage/prod, and the playbook for responding to incidents. Use it as the authoritative source when coordinating with security or platform operations, while noting that SKUs and deployment footprints will be revisited after performance and cost testing.

```mermaid
graph TB
    subgraph "Azure Cloud - Production Environment"
        subgraph "Region: West US 2"
            subgraph "Virtual Network: endymion-ai-vnet"
                subgraph "Subnet: frontend-subnet"
                    CDN["Azure CDN<br/>Global Edge Distribution"]
                    STATICAPP[Azure Static Web Apps<br/>React SPA Hosting<br/>SKU: Standard]
                end
                
                subgraph "Subnet: app-backend-subnet"
                    APIMGMT[Azure API Management<br/>Gateway Layer<br/>SKU: Developer<br/>Custom Domain: api.endymion-ai.com]
                    FUNC[Azure Functions<br/>App Backend<br/>Consumption Plan<br/>Python 3.11 Runtime]
                end
                
                subgraph "Subnet: data-subnet"
                    SQL[Azure SQL Database<br/>App Database<br/>Serverless 2 vCores<br/>Auto-pause: 1 hour]
                    ADLS[Azure Data Lake Storage Gen2<br/>500 GB Hot Tier<br/>Hierarchical Namespace<br/>Bronze/Silver/Gold Containers]
                end
                
                subgraph "Subnet: databricks-subnet (VNet Injection)"
                    DBW[Azure Databricks Workspace<br/>Standard Tier<br/>Unity Catalog Enabled]
                    DBWC["Databricks Clusters<br/>4-16 Workers (Auto-scaling)<br/>DBR 13.3 LTS<br/>Spot Instances"]
                end
                
                subgraph "Subnet: security-subnet"
                    KV[Azure Key Vault<br/>Secrets Management<br/>Standard Tier<br/>Soft Delete Enabled]
                    ENTRAID[EntraID B2C Tenant<br/>User/Tenant Repository<br/>50K MAU Free Tier]
                end
                
                subgraph "Subnet: monitoring-subnet"
                    MONITOR[Azure Monitor<br/>Application Insights<br/>Log Analytics Workspace<br/>90-day Retention]
                end
            end
            
            NSG[Network Security Group<br/>Inbound: HTTPS only<br/>Outbound: Restricted]
        end
        
        subgraph "External Integrations"
            BC[Business Central<br/>On-Premise/Cloud<br/>REST API]
            MEQ[MEQ Snowflake<br/>Shared Dataset<br/>REST API]
            EV[E+V Imaging<br/>Cloud SaaS<br/>REST API]
            FEEDLOT[Feedlot Systems<br/>SFTP/CSV<br/>Multiple Partners]
        end
    end
    
    subgraph "Developer Workstations"
        DEVRM[Romania Team<br/>App Development<br/>VS Code + Git]
        DEVMEX[Gracchus Team<br/>Platform Development<br/>VS Code + Databricks]
        DEVUX[Gracchus UX Team<br/>Figma Design<br/>Handoff to Romania]
    end
    
    subgraph "CI/CD Pipeline"
        ADO[Azure DevOps<br/>Git Repos + Pipelines<br/>YAML Definitions]
    end
    
    %% User Access
    USER[End Users<br/>Ranch Managers<br/>Analysts<br/>Executives] --> CDN
    CDN --> STATICAPP
    STATICAPP --> APIMGMT
    
    %% App Backend Flow
    APIMGMT --> FUNC
    FUNC --> SQL
    FUNC --> ENTRAID
    FUNC --> KV
    
    %% Platform API Flow
    APIMGMT --> DBW
    DBW --> ADLS
    DBW --> KV
    DBW --> DBWC
    
    %% External Data Ingestion
    BC --> APIMGMT
    MEQ --> APIMGMT
    EV --> APIMGMT
    FEEDLOT --> ADLS
    
    APIMGMT --> DBW
    
    %% Monitoring
    STATICAPP --> MONITOR
    FUNC --> MONITOR
    DBW --> MONITOR
    
    %% Security
    NSG --> STATICAPP
    NSG --> FUNC
    NSG --> DBW
    
    %% Development Flow
    DEVRM --> ADO
    DEVMEX --> ADO
    DEVUX --> DEVRM
    ADO --> STATICAPP
    ADO --> FUNC
    ADO --> DBW
    
    %% Styling
    classDef azure fill:#E3F2FD,stroke:#1976D2,stroke-width:2px,color:#000
    classDef external fill:#FFF3E0,stroke:#F57C00,stroke-width:2px,color:#000
    classDef dev fill:#E8F5E9,stroke:#388E3C,stroke-width:2px,color:#000
    classDef security fill:#FCE4EC,stroke:#C2185B,stroke-width:2px,color:#000
    
    class CDN,STATICAPP,APIMGMT,FUNC,SQL,ADLS,DBW,DBWC,MONITOR,NSG azure
    class BC,MEQ,EV,FEEDLOT external
    class DEVRM,DEVMEX,DEVUX,ADO dev
    class KV,ENTRAID security
```

### Infrastructure Specifications

| Component | Azure Service | SKU/Tier | Rationale |
|-----------|--------------|----------|-----------|
| **Frontend Hosting** | Azure Static Web Apps | Standard | Global CDN, auto-HTTPS, git integration |
| **API Gateway** | Azure API Management | Developer (MVP) | Rate limiting, OAuth validation, API routing |
| **App Backend** | Azure Functions | Consumption Plan | Serverless auto-scaling, pay-per-execution |
| **App Database** | Azure SQL Database | Serverless, 2 vCores | Auto-pause for cost savings, suitable for <100 users |
| **Data Lake** | ADLS Gen2 | Hot Tier, 500 GB | Hierarchical namespace for Delta Lake |
| **Big Data Processing** | Azure Databricks | Standard, Auto-scaling | Unified lakehouse for data + ML |
| **Secrets Management** | Azure Key Vault | Standard | API keys, connection strings, certificates |
| **Authentication** | EntraID B2C | Free (50K MAU) | Enterprise SSO, MFA, tenant isolation |
| **Monitoring** | Azure Monitor | Pay-as-you-go | Application Insights, Log Analytics |

### Network Security

| Layer | Protection | Implementation |
|-------|-----------|----------------|
| **Perimeter** | DDoS Protection | Azure DDoS Standard (future) |
| **Network** | Network Security Groups | Allow HTTPS (443), deny all others |
| **API** | Rate Limiting | APIM: 1000 req/min per user |
| **Data** | Private Endpoints | SQL, Storage, Databricks use private IPs |
| **Identity** | MFA | EntraID B2C enforces MFA for all users |
| **Secrets** | Key Vault | No credentials in code; managed identities |

---

## View 5: Scenarios (Use Case View)

### Purpose
Ties together the other views by showing **how key use cases** flow through the architecture. This validates that the architecture supports business requirements.

Each scenario emphasizes a business outcome that Endymion-AI Livestock cares about and demonstrates how the platform's components collaborate to deliver it. By mapping user journeys end-to-end, we ensure that cross-team contracts (data freshness, API semantics, auth flows) are explicitly validated. The section is also a living backlog for automated testing and monitoring, and the scenarios themselves will evolve as new insights surface during pilot usage or scope changes.

```mermaid
graph LR
    subgraph "Scenario 1: Daily Operations - Harvest Planning"
        S1A[Ranch Manager<br/>Logs In] --> S1B[Authenticates<br/>via EntraID]
        S1B --> S1C[Loads Harvest<br/>Scheduler UI]
        S1C --> S1D[Requests CPI-Ranked<br/>Candidates via API]
        S1D --> S1E[Platform Returns<br/>Predictions from<br/>Feature Store]
        S1E --> S1F[Displays Animals<br/>Ready for Harvest]
        S1F --> S1G[Adjusts Harvest<br/>Dates via Drag-Drop]
        S1G --> S1H[Saves Schedule<br/>to App Database]
        S1H --> S1I[Exports to<br/>Feedlot System]
    end
    
    subgraph "Scenario 2: Model Training & Deployment"
        S2A[Data Scientist<br/>Reviews Model<br/>Accuracy Drop Alert] --> S2B[Triggers Retraining<br/>Pipeline in Databricks]
        S2B --> S2C[Loads Training Data<br/>from Feature Store]
        S2C --> S2D[Trains New<br/>Marbling Model<br/>XGBoost]
        S2D --> S2E[Validates on<br/>Holdout Set]
        S2E --> S2F{Accuracy<br/>Improved?}
        S2F -->|Yes| S2G[Registers Model<br/>in MLflow]
        S2F -->|No| S2H[Logs Failure<br/>Investigates Features]
        S2G --> S2I[Runs A/B Test<br/>Shadow Scoring]
        S2I --> S2J[Promotes to<br/>Production]
        S2J --> S2K[Platform Uses<br/>New Model for<br/>Predictions]
    end
    
    subgraph "Scenario 3: New Tenant Onboarding"
        S3A[Admin User<br/>Creates New Tenant<br/>in Admin Module] --> S3B[Writes Tenant<br/>to EntraID<br/>via LDAP]
        S3B --> S3C[EntraID Generates<br/>Tenant ID]
        S3C --> S3D[App Creates<br/>Tenant Record<br/>in App DB]
        S3D --> S3E[Links via<br/>EntraID Tenant ID]
        S3E --> S3F[Admin Adds<br/>Users to Tenant]
        S3F --> S3G[Users Receive<br/>Invite Emails]
        S3G --> S3H[Users Authenticate<br/>See Tenant-Specific<br/>Data Only]
    end
    
    subgraph "Scenario 4: Data Ingestion & Feature Refresh"
        S4A[Hourly Trigger<br/>2:00 PM] --> S4B[ADF Pipeline<br/>Fetches MEQ Data]
        S4B --> S4C[Writes to<br/>Bronze Layer<br/>Delta Lake]
        S4C --> S4D[Data Quality<br/>Validation]
        S4D --> S4E{Quality<br/>Check Pass?}
        S4E -->|Yes| S4F[Transforms to<br/>Silver Layer]
        S4E -->|No| S4G[Alerts Data<br/>Engineering Team]
        S4F --> S4H[Triggers Feature<br/>Engineering Pipeline]
        S4H --> S4I[Updates Feature<br/>Store with Latest<br/>MEQ Values]
        S4I --> S4J[Feature Store<br/>Available for<br/>Next Prediction]
    end
    
    subgraph "Scenario 5: NBA Recommendation Approval"
        S5A[Operations Manager<br/>Opens NBA Dashboard] --> S5B[Sees Top 20<br/>Recommendations<br/>Ranked by ROI]
        S5B --> S5C[Reviews<br/>'Harvest Animal 12345']
        S5C --> S5D[Clicks Approve]
        S5D --> S5E[App Backend<br/>Validates Permission]
        S5E --> S5F[Platform Updates<br/>Animal Status<br/>in Feature Store]
        S5F --> S5G[Logs Approval<br/>Event with Timestamp]
        S5G --> S5H[Updates NBA<br/>Acceptance Rate<br/>Metric]
        S5H --> S5I[Sends Notification<br/>to Feedlot System]
    end
    
    style S1A fill:#B3E5FC
    style S2A fill:#C8E6C9
    style S3A fill:#FFE0B2
    style S4A fill:#F8BBD0
    style S5A fill:#D1C4E9
```

### Scenario Summary

| Scenario | Primary Actors | Key Systems | Business Value |
|----------|----------------|-------------|----------------|
| **1. Harvest Planning** | Ranch Manager | App UI, Platform Scheduler, Feature Store | Optimize harvest timing → reduce feed costs |
| **2. Model Training** | Data Scientist | Databricks, MLflow, Feature Store | Maintain model accuracy → reliable predictions |
| **3. Tenant Onboarding** | Admin User | Admin Module, EntraID, App DB | Enable multi-tenant SaaS → scalability |
| **4. Data Ingestion** | Automated System | ADF, Databricks, Delta Lake | Fresh data → current predictions |
| **5. NBA Approval** | Operations Manager | App UI, Platform APIs, Feature Store | Act on recommendations → ROI realization |

---

## Team Execution View (Collaboration Model)

The collaboration model keeps responsibilities crisp while giving both teams shared governance for cross-cutting concerns. Assignments are still in draft, yet both sides agree on the working principles so we can iterate quickly without blurring accountability.

- **Gracchus team** owns the Endymion-AI Platform end-to-end (data, ML, APIs, identity, Scheduler/NBA/Reporting engines) and leads production readiness for platform services.
- **Romania team** owns the Endymion-AI App (frontend, backend services, admin experience) and leads user-facing release management.

Both workstreams live in a single GitHub repository to simplify dependency management and code reuse. The repo inherits common policies for issue tracking, escalation, and conflict resolution so there is one source of truth when blockers surface. Within that shared governance, each team manages its own Kanban board, issue triage, and delivery cadences to keep platform and app velocity aligned while respecting local priorities.

These team boundaries will be reviewed during the pilot phase; any shifts in scope or runbook ownership will have to be agreed and signed off by all levels of this project, with the corresponding upstream and downstream changes in architecture.

```mermaid
graph LR
    subgraph "Shared GitHub Repository"
        PLATFORM["Endymion-AI Platform<br>Code Owner: Gracchus<br>Kanban: Platform Board"]
        APP["Endymion-AI App<br>Code Owner: Romania<br>Kanban: App Board"]
    end

    GOVERNANCE["Shared Governance<br>Issue Tracking, Escalation, Conflict Resolution"]

    PLATFORM --> GOVERNANCE
    APP --> GOVERNANCE
    GOVERNANCE --> PLATFORM
    GOVERNANCE --> APP
    PLATFORM -.coordinate.- APP
```


