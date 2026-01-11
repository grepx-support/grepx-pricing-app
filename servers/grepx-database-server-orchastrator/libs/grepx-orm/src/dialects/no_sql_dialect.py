"""NoSQL dialect for document databases"""

from typing import List, Dict, Any


class NoSQLDialect:
    """NoSQL dialect implementation for document databases like MongoDB"""

    def to_mongo_format(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Python data types to MongoDB compatible types"""
        result = {}
        for key, value in data.items():
            if isinstance(value, dict):
                result[key] = self.to_mongo_format(value)
            elif hasattr(value, 'to_dict'):
                result[key] = value.to_dict()
            else:
                result[key] = value
        return result

    def from_mongo_format(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert MongoDB data to Python types"""
        result = {}
        for key, value in data.items():
            if key == '_id':
                # Convert ObjectId to string for easier handling
                result['_id'] = str(value)
            elif isinstance(value, dict):
                result[key] = self.from_mongo_format(value)
            elif isinstance(value, list):
                result[key] = [
                    self.from_mongo_format(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                result[key] = value
        return result

    def build_mongo_query(self, filters: List[tuple]) -> Dict[str, Any]:
        """Convert ORM filters to MongoDB query format"""
        query = {}
        for field, operator, value in filters:
            mongo_operator = self._convert_operator(operator)
            if mongo_operator == '$eq':
                query[field] = value
            else:
                if field not in query:
                    query[field] = {}
                query[field][mongo_operator] = value
        return query

    def _convert_operator(self, operator: str) -> str:
        """Convert SQL operator to MongoDB operator"""
        operator_map = {
            '=': '$eq',
            '!=': '$ne',
            '>': '$gt',
            '>=': '$gte',
            '<': '$lt',
            '<=': '$lte',
            'in': '$in',
            'not in': '$nin'
        }
        return operator_map.get(operator, '$eq')

    def build_sort_spec(self, order_by: List[str]) -> List[tuple]:
        """Convert order_by to MongoDB sort specification"""
        sort_spec = []
        for field in order_by:
            if field.startswith('-'):
                sort_spec.append((field[1:], -1))
            else:
                sort_spec.append((field, 1))
        return sort_spec
