"""
Simplified MCP Server implementation for Neo4j schema exploration.
This is a lightweight version focused on Neo4j database schema exposure for Cypher generation.
"""

import json
import logging
import time
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class JSONRPCRequest:
    """JSON-RPC 2.0 request format."""
    jsonrpc: str
    method: str
    params: Optional[Union[Dict[str, Any], list]] = None
    id: Optional[Union[str, int]] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JSONRPCRequest':
        return cls(
            jsonrpc=data.get('jsonrpc', '2.0'),
            method=data['method'],
            params=data.get('params'),
            id=data.get('id')
        )
    
    def is_notification(self) -> bool:
        return self.id is None


@dataclass
class JSONRPCResponse:
    """JSON-RPC 2.0 response format."""
    jsonrpc: str = "2.0"
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None
    id: Optional[Union[str, int]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        response = {"jsonrpc": self.jsonrpc}
        
        if self.error:
            response["error"] = self.error
        else:
            response["result"] = self.result
        
        if self.id is not None:
            response["id"] = self.id
            
        return response


class MCPTool:
    """Base class for MCP tools."""
    
    def __init__(self, name: str, description: str, parameters: List[Dict[str, Any]] = None):
        self.name = name
        self.description = description
        self.parameters = parameters or []
    
    def get_schema(self) -> Dict[str, Any]:
        """Get tool schema in MCP format."""
        properties = {}
        required = []
        
        for param in self.parameters:
            param_name = param['name']
            properties[param_name] = {
                'type': param['type'],
                'description': param['description']
            }
            
            if param.get('enum'):
                properties[param_name]['enum'] = param['enum']
            
            if param.get('required', False):
                required.append(param_name)
        
        return {
            'name': self.name,
            'description': self.description,
            'inputSchema': {
                'type': 'object',
                'properties': properties,
                'required': required
            }
        }
    
    def execute(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the tool. To be implemented by subclasses."""
        raise NotImplementedError


class MCPServer:
    """Simplified MCP Server for Text-to-SQL applications."""
    
    def __init__(self):
        self.tools: Dict[str, MCPTool] = {}
        self.server_info = {
            'name': 'Neo4j Schema MCP Server',
            'version': '1.0.0',
            'description': 'Neo4j database schema exposure for Cypher/text-to-SQL generation'
        }
    
    def register_tool(self, tool: MCPTool) -> None:
        """Register a tool with the server."""
        self.tools[tool.name] = tool
        logger.info(f"Registered tool: {tool.name}")
    
    def list_tools(self) -> List[Dict[str, Any]]:
        """List all available tools."""
        return [tool.get_schema() for tool in self.tools.values()]
    
    def get_server_info(self) -> Dict[str, Any]:
        """Get server information."""
        return {
            **self.server_info,
            'tools_count': len(self.tools),
            'capabilities': {
                'tools': True,
                'resources': False,
                'prompts': False
            }
        }
    
    def handle_request(self, request_data: Union[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Handle JSON-RPC request."""
        try:
            # Parse request
            if isinstance(request_data, str):
                try:
                    request_dict = json.loads(request_data)
                except json.JSONDecodeError as e:
                    return self._create_error_response(-32700, "Parse error", str(e))
            else:
                request_dict = request_data
            
            # Create request object
            try:
                request = JSONRPCRequest.from_dict(request_dict)
            except KeyError as e:
                return self._create_error_response(-32600, "Invalid Request", f"Missing field: {e}")
            
            # Handle request
            response = self._dispatch_method(request)
            
            # Don't return response for notifications
            if request.is_notification():
                return None
            
            return response.to_dict()
            
        except Exception as e:
            logger.error(f"Request handling error: {str(e)}")
            return self._create_error_response(-32603, "Internal error", str(e))
    
    def _dispatch_method(self, request: JSONRPCRequest) -> JSONRPCResponse:
        """Dispatch method to appropriate handler."""
        method_handlers = {
            'initialize': self._handle_initialize,
            'tools/list': self._handle_list_tools,
            'tools/call': self._handle_call_tool,
            'server/info': self._handle_server_info
        }
        
        if request.method not in method_handlers:
            return JSONRPCResponse(
                error={"code": -32601, "message": f"Method not found: {request.method}"},
                id=request.id
            )
        
        try:
            handler = method_handlers[request.method]
            result = handler(request.params or {})
            return JSONRPCResponse(result=result, id=request.id)
            
        except Exception as e:
            logger.error(f"Error in method {request.method}: {str(e)}")
            return JSONRPCResponse(
                error={"code": -32603, "message": f"Error executing {request.method}", "data": str(e)},
                id=request.id
            )
    
    def _create_error_response(self, code: int, message: str, data: Any = None) -> Dict[str, Any]:
        """Create error response."""
        error = {"code": code, "message": message}
        if data:
            error["data"] = data
        return JSONRPCResponse(error=error).to_dict()
    
    def _handle_initialize(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle initialize request."""
        return {
            "protocolVersion": params.get('protocolVersion', '2024-11-05'),
            "capabilities": {
                "tools": {}
            },
            "serverInfo": self.server_info
        }
    
    def _handle_list_tools(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tools/list request."""
        return {"tools": self.list_tools()}
    
    def _handle_call_tool(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tools/call request."""
        if 'name' not in params:
            raise ValueError("Missing required parameter: name")
        
        tool_name = params['name']
        tool_params = params.get('arguments', {})
        
        if tool_name not in self.tools:
            raise ValueError(f"Tool not found: {tool_name}")
        
        try:
            tool = self.tools[tool_name]
            result = tool.execute(tool_params)
            
            # Format response as MCP content
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(result, indent=2, default=str)
                    }
                ]
            }
            
        except Exception as e:
            logger.error(f"Tool execution error for {tool_name}: {str(e)}")
            raise Exception(f"Tool execution failed: {str(e)}")
    
    def _handle_server_info(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle server/info request."""
        return self.get_server_info()
