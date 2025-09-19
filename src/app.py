#!/usr/bin/env python3
"""
Neo4j Schema MCP Server
A simplified Model Context Protocol server that exposes Neo4j database schema
for text-to-SQL (Cypher) generation applications.

Usage: python app.py
"""

import os
import json
import logging
from flask import Flask, request, jsonify
from dotenv import load_dotenv

from mcp_server import MCPServer
from tools.neo4j_schema_tool import Neo4jSchemaTool
from tools.neo4j_query_tool import Neo4jQueryTool

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Initialize MCP Server
mcp_server = MCPServer()

def initialize_tools():
    """Initialize and register all available tools."""
    try:
        # Register Neo4j schema tool
        neo4j_schema_tool = Neo4jSchemaTool()
        mcp_server.register_tool(neo4j_schema_tool)
        logger.info("Registered Neo4j schema tool")
        
        # Register Neo4j query tool
        neo4j_query_tool = Neo4jQueryTool()
        mcp_server.register_tool(neo4j_query_tool)
        logger.info("Registered Neo4j query tool")
        
        logger.info(f"MCP Server initialized with {len(mcp_server.tools)} tools")
        
    except Exception as e:
        logger.error(f"Error initializing tools: {str(e)}")
        raise

@app.route('/mcp', methods=['POST'])
def mcp_endpoint():
    """Main MCP endpoint for JSON-RPC requests."""
    try:
        # Parse JSON request
        if not request.is_json:
            return jsonify({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32700,
                    "message": "Parse error",
                    "data": "Content-Type must be application/json"
                }
            }), 400
        
        request_data = request.get_json()
        logger.info(f"MCP Request: {request_data.get('method', 'unknown')}")
        
        # Handle JSON-RPC request through MCP server
        response = mcp_server.handle_request(request_data)
        
        if response is None:
            # Notification request (no response expected)
            return '', 204
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"MCP endpoint error: {str(e)}")
        return jsonify({
            "jsonrpc": "2.0",
            "error": {
                "code": -32603,
                "message": "Internal error",
                "data": str(e)
            }
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "server": "Neo4j Schema MCP Server",
        "version": "1.0.0",
        "tools_registered": len(mcp_server.tools)
    })

@app.route('/info', methods=['GET'])
def server_info():
    """Get server information and capabilities."""
    return jsonify(mcp_server.get_server_info())

@app.route('/tools', methods=['GET'])
def list_tools():
    """List all available tools."""
    return jsonify({
        "tools": mcp_server.list_tools()
    })

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error": "Not found",
        "message": "Use POST /mcp for MCP requests, GET /health for status"
    }), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        "error": "Method not allowed",
        "message": "Use POST /mcp for MCP requests"
    }), 405

if __name__ == '__main__':
    try:
        # Initialize tools
        initialize_tools()
        
        # Get configuration
        host = os.getenv('FLASK_HOST', '0.0.0.0')
        port = int(os.getenv('FLASK_PORT', '5001'))
        debug = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
        
        logger.info(f"Starting Neo4j Schema MCP Server on {host}:{port}")
        logger.info(f"MCP endpoint: http://{host}:{port}/mcp")
        logger.info(f"Health check: http://{host}:{port}/health")
        
        # Start Flask app
        app.run(
            host=host,
            port=port,
            debug=debug,
            threaded=True
        )
        
    except Exception as e:
        logger.error(f"Failed to start server: {str(e)}")
        exit(1)
