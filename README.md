# PostgreSQL Text-to-SQL MCP Server

A production-ready Model Context Protocol (MCP) server that extracts PostgreSQL schema into Neo4j and exposes it via **two powerful tools** for accurate PostgreSQL SQL query generation.

## 🚀 Key Features

- **📊 Structured Schema Exploration**: Get organized views of tables, columns, relationships, and constraints
- **🔍 Raw Cypher Query Execution**: Full freedom to explore schema with custom Cypher queries  
- **🔒 Security First**: Read-only operations with query validation to prevent dangerous actions
- **🎯 SQL Generation Focus**: Designed specifically for PostgreSQL text-to-SQL applications
- **⚡ Neo4j Knowledge Graph**: Fast relationship discovery and complex schema navigation

## Architecture

```
PostgreSQL (Pagila DB) → Schema Parser → Neo4j Knowledge Graph → MCP Server → AI Agents → PostgreSQL SQL Generation
                                                                      ↓
                                                            [Schema Tool + Query Tool]
```

## Quick Start

### 1. Environment Setup

Create `.env` file:

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=postgres

NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=neo4j

# MCP Server
FLASK_PORT=5001
```

### 2. Start Services

```bash
# Start databases (PostgreSQL + Neo4j with APOC plugin)
docker-compose up -d

# Install dependencies
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Parse PostgreSQL schema into Neo4j
python scripts/01-parse_schema.py

# Start MCP server
python src/app.py
```

## MCP Config example

```json
{
  "mcpServers": {
    "neo4j-schema": {
      "url": "http://localhost:5001/mcp"
    }
  }
}
```

## Available MCP Tools

### 🔧 Tool 1: `neo4j_schema` - Structured Schema Exploration

Get organized PostgreSQL schema information for SQL generation.

**Parameters:**
- `schema_type` (required): `overview | nodes | relationships | properties | sample_data`
- `node_label` (optional): Filter by entity type (`Table`, `Column`, `Constraint`, `Index`)
- `limit` (optional): Maximum results for sample data

**Examples:**
```json
{"schema_type": "overview"}
→ Database statistics: 29 tables, 171 columns, 502 relationships

{"schema_type": "nodes"} 
→ Entity counts: Tables(29), Columns(171), Constraints(172), Indexes(55)

{"schema_type": "relationships"}
→ Relationship types: HAS_COLUMN(171), REFERENCES(36), HAS_CONSTRAINT(172)

{"schema_type": "properties", "node_label": "Table"}
→ Properties available on Table entities
```

### 🔍 Tool 2: `neo4j_query` - Raw Cypher Execution

Execute custom Cypher queries for advanced schema exploration.

**Parameters:**
- `cypher_query` (required): Read-only Cypher query to explore PostgreSQL schema
- `limit` (optional): Maximum results (default: 100, max: 1000)
- `include_query_plan` (optional): Include execution plan for optimization

**Examples:**
```cypher
# List all tables
MATCH (t:Table) RETURN t.table_name ORDER BY t.table_name

# Get table schema with column details
MATCH (t:Table {table_name: 'customer'})-[:HAS_COLUMN]->(c:Column) 
RETURN c.column_name, c.data_type, c.is_nullable 
ORDER BY c.ordinal_position

# Discover foreign key relationships
MATCH (c1:Column)-[:REFERENCES]->(c2:Column) 
RETURN c1.table_name as from_table, c1.column_name as from_column,
       c2.table_name as to_table, c2.column_name as to_column

# Find tables with specific column types
MATCH (t:Table)-[:HAS_COLUMN]->(c:Column {data_type: 'text'}) 
RETURN DISTINCT t.table_name, count(c) as text_columns 
ORDER BY text_columns DESC
```

## 🎯 Agent Usage Patterns

### Pattern 1: Schema Discovery
1. **Start broad**: `{"schema_type": "overview"}` → Understand database scope
2. **Explore entities**: `{"schema_type": "nodes"}` → See all table/column counts  
3. **Map relationships**: `{"schema_type": "relationships"}` → Understand FK structure

### Pattern 2: Table-Specific Exploration
1. **List tables**: `MATCH (t:Table) RETURN t.table_name`
2. **Get table schema**: `MATCH (t:Table {table_name: 'X'})-[:HAS_COLUMN]->(c:Column) RETURN c.*`
3. **Find related tables**: `MATCH (c1:Column)-[:REFERENCES]->(c2:Column) WHERE c1.table_name = 'X' RETURN c2.table_name`

### Pattern 3: SQL Generation Workflow
1. **Understand request** → Identify required tables
2. **Explore schema** → Get column names, types, nullability
3. **Map relationships** → Find JOIN conditions via foreign keys
4. **Generate SQL** → Use discovered schema to build accurate queries

## 📊 Sample Database

The server comes pre-configured with the **Pagila PostgreSQL database** (PostgreSQL version of Sakila):
- **29 tables**: actor, customer, film, rental, payment, etc.
- **171 columns**: Complete with data types, constraints, defaults
- **36 foreign keys**: Proper relationship mapping for JOINs
- **Real-world complexity**: Multi-table relationships, indexes, constraints

## Project Structure

```
├── README.md
├── docker-compose.yml              # PostgreSQL + Neo4j services
├── requirements.txt                # Python dependencies
├── scripts/
│   ├── 00-clean_neo4j_database.py  # Reset Neo4j database
│   └── 01-parse_schema.py          # Parse PostgreSQL schema to Neo4j
└── src/                            # MCP Server implementation
    ├── app.py                      # Flask MCP server entry point
    ├── mcp_server.py               # Core MCP protocol implementation
    └── tools/
        ├── __init__.py
        ├── neo4j_schema_tool.py    # Structured schema exploration
        └── neo4j_query_tool.py     # Raw Cypher query execution
```

## 🔒 Security Features

- **Read-only operations**: No schema modifications allowed
- **Query validation**: Blocks dangerous operations (CREATE, DELETE, DROP, etc.)
- **Input sanitization**: Prevents injection attacks
- **Connection limits**: Configurable result limits to prevent resource exhaustion

## 🛠️ Development

### Reset Neo4j Database
```bash
python scripts/00-clean_neo4j_database.py
python scripts/01-parse_schema.py  # Re-parse schema
```

### Test MCP Tools
```bash
# Test schema tool
curl "http://localhost:5001/mcp" -X POST \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc": "2.0", "method": "tools/call", "params": {"name": "neo4j_schema", "arguments": {"schema_type": "overview"}}, "id": 1}'

# Test query tool  
curl "http://localhost:5001/mcp" -X POST \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc": "2.0", "method": "tools/call", "params": {"name": "neo4j_query", "arguments": {"cypher_query": "MATCH (t:Table) RETURN count(t)"}}, "id": 1}'
```

## 📈 Performance

- **Neo4j APOC**: Enhanced performance for complex queries
- **Connection pooling**: Efficient database connections
- **Query optimization**: Built-in query plans and optimization hints
- **Caching**: Schema information cached for repeated access

## 🎯 Use Cases

- **AI-powered SQL generation**: Agents understand schema to generate accurate queries
- **Database documentation**: Automated schema exploration and documentation
- **Data discovery**: Find tables, relationships, and column information
- **Schema analysis**: Understand database structure and relationships
- **Query optimization**: Use relationship data to optimize JOIN operations

---

**Ready to build intelligent PostgreSQL text-to-SQL applications!** 🚀

The MCP server provides everything AI agents need to understand PostgreSQL schema and generate accurate SQL queries through two complementary tools: structured exploration and raw query execution.