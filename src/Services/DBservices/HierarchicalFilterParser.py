# src/Services/DBservices/HierarchicalFilterParser.py

from typing import Any, Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class HierarchicalFilterParser:
    """
    Parse and transform hierarchical filter structures
    
    Responsibilities:
    - Parse nested filter hierarchies
    - Extract tags and synonyms
    - Build search queries
    - Transform filter selections
    """
    
    def __init__(self, filter_structure: Dict[str, Any]):
        """
        Initialize parser with filter structure
        
        Args:
            filter_structure: Dict with 'others' and 'tag_filters' keys
        """
        self.filter_structure = filter_structure
        self.others = filter_structure.get('others', {})
        self.tag_filters = filter_structure.get('tag_filters', {})
        self.logger = logger
        
        self.logger.info("HierarchicalFilterParser initialized")
    
    def get_filter_tree(self) -> Dict[str, Any]:
        """
        Get complete hierarchical filter tree
        
        Returns:
        {
            'basic_filters': {...},
            'tag_filters': {...},
            'total_categories': int
        }
        """
        try:
            tree = {
                'basic_filters': self._build_basic_tree(),
                'tag_filters': self._build_tag_tree(),
                'total_categories': 0
            }
            
            tree['total_categories'] = len(tree['basic_filters']) + len(tree['tag_filters'])
            
            self.logger.info(f"Filter tree built with {tree['total_categories']} categories")
            return tree
        
        except Exception as e:
            self.logger.error(f"Error building filter tree: {str(e)}")
            return {'basic_filters': {}, 'tag_filters': {}, 'total_categories': 0}
    
    def _build_basic_tree(self) -> Dict[str, Any]:
        """Build tree for basic (simple) filters"""
        tree = {}
        
        for key, values in self.others.items():
            if isinstance(values, dict):
                # Already has structure
                tree[key] = values
            else:
                # Convert list to structured format
                tree[key] = {
                    'type': 'select',
                    'count': len(values) if isinstance(values, list) else 0,
                    'values': values if isinstance(values, list) else []
                }
        
        return tree
    
    def _build_tag_tree(self) -> Dict[str, Any]:
        """Build tree for tag filters"""
        tree = {}
        
        for category, groups in self.tag_filters.items():
            tree[category] = {
                'type': 'category',
                'subcategories': {}
            }
            
            if isinstance(groups, dict):
                for group_key, options in groups.items():
                    tree[category]['subcategories'][group_key] = {}
                    
                    if isinstance(options, dict):
                        for opt_key, opt_value in options.items():
                            tree[category]['subcategories'][group_key][opt_key] = {
                                'display': opt_value.get('display', opt_key) if isinstance(opt_value, dict) else opt_value,
                                'type': 'tag'
                            }
        
        return tree
    
    def get_all_tags(self) -> Dict[str, Any]:
        """
        Get all tags flattened (not hierarchical)
        
        Returns:
        {
            'gender.sex': {
                'display': 'sex',
                'synonyms': ['male:male', 'female:female', ...]
            },
            ...
        }
        """
        try:
            tags = {}
            
            for category, groups in self.tag_filters.items():
                if not isinstance(groups, dict):
                    continue
                
                for group_key, options in groups.items():
                    if not isinstance(options, dict):
                        continue
                    
                    for opt_key, opt_value in options.items():
                        if isinstance(opt_value, dict):
                            path = f"{category}.{group_key}.{opt_key}" if group_key else f"{category}.{opt_key}"
                            tags[path] = {
                                'display': opt_value.get('display', opt_key),
                                'synonyms': opt_value.get('synonyms', [opt_key])
                            }
            
            self.logger.info(f"Found {len(tags)} total tags")
            return tags
        
        except Exception as e:
            self.logger.error(f"Error getting all tags: {str(e)}")
            return {}
    
    def search_tags(self, search_term: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Search tags by term or synonym
        
        Args:
            search_term: Term to search for
            limit: Maximum results
        
        Returns: List of matching tags
        """
        try:
            if not search_term:
                return []
            
            search_lower = search_term.lower()
            matches = []
            
            all_tags = self.get_all_tags()
            
            for path, tag_info in all_tags.items():
                # Check display name
                if search_lower in tag_info.get('display', '').lower():
                    matches.append({
                        'path': path,
                        'display': tag_info['display'],
                        'type': 'tag',
                        'synonyms': tag_info.get('synonyms', [])
                    })
                
                # Check synonyms
                else:
                    for synonym in tag_info.get('synonyms', []):
                        if search_lower in synonym.lower():
                            matches.append({
                                'path': path,
                                'display': tag_info['display'],
                                'matched_synonym': synonym,
                                'type': 'tag',
                                'synonyms': tag_info.get('synonyms', [])
                            })
                            break
                
                if len(matches) >= limit:
                    break
            
            self.logger.info(f"Found {len(matches)} matches for '{search_term}'")
            return matches
        
        except Exception as e:
            self.logger.error(f"Error searching tags: {str(e)}")
            return []
    
    def get_filters_by_category(self, category: str) -> Dict[str, Any]:
        """
        Get all filters in a category
        
        Args:
            category: Category name (e.g., 'intervention', 'outcome')
        
        Returns: Category filters
        """
        try:
            # Check tag filters first
            if category in self.tag_filters:
                result = {}
                groups = self.tag_filters[category]
                
                if isinstance(groups, dict):
                    for group_key, options in groups.items():
                        if isinstance(options, dict):
                            for opt_key, opt_value in options.items():
                                result[opt_key] = {
                                    'path': f"{category}.{group_key}.{opt_key}",
                                    'display': opt_value.get('display', opt_key) if isinstance(opt_value, dict) else opt_value,
                                    'synonyms': opt_value.get('synonyms', []) if isinstance(opt_value, dict) else []
                                }
                
                return result
            
            # Check basic filters
            elif category in self.others:
                values = self.others[category]
                if isinstance(values, list):
                    return {v: {'value': v, 'display': v} for v in values}
            
            return {}
        
        except Exception as e:
            self.logger.error(f"Error getting filters for category '{category}': {str(e)}")
            return {}
    
    def build_search_query(self, selections: Dict[str, List[Any]]) -> Dict[str, Any]:
        """
        Build search query from filter selections
        
        Input:
        {
            'gender.sex': ['male', 'female'],
            'intervention.vpd.covid': ['covid'],
            'year': [2020, 2021, 2022],
            'country': ['USA', 'Canada']
        }
        
        Output:
        {
            'conditions': [
                {field: 'abstract', operator: 'contains', value: 'male'},
                {field: 'abstract', operator: 'contains', value: 'female'},
                {field: 'year', operator: 'in', values: [2020, 2021, 2022]},
                {field: 'country', operator: 'in', values: ['USA', 'Canada']}
            ],
            'logic': 'AND'
        }
        """
        try:
            conditions = []
            
            for path, values in selections.items():
                if not values:
                    continue
                
                # Convert to list if single value
                if not isinstance(values, list):
                    values = [values]
                
                # Handle tag filters (hierarchical paths)
                if '.' in path:
                    for value in values:
                        # Add conditions for title and abstract search
                        conditions.append({
                            'field': 'title',
                            'operator': 'contains',
                            'value': value
                        })
                        conditions.append({
                            'field': 'abstract',
                            'operator': 'contains',
                            'value': value
                        })
                
                # Handle basic filters
                else:
                    conditions.append({
                        'field': path,
                        'operator': 'in' if len(values) > 1 else '=',
                        'values': values if len(values) > 1 else None,
                        'value': values[0] if len(values) == 1 else None
                    })
            
            query = {
                'conditions': conditions,
                'logic': 'AND'
            }
            
            self.logger.info(f"Built search query with {len(conditions)} conditions")
            return query
        
        except Exception as e:
            self.logger.error(f"Error building search query: {str(e)}")
            return {'conditions': [], 'logic': 'AND'}
    
    def validate_selections(self, selections: Dict[str, List[Any]]) -> Tuple[bool, List[str]]:
        """
        Validate filter selections
        
        Returns: (is_valid, [error_messages])
        """
        errors = []
        
        if not isinstance(selections, dict):
            errors.append("Selections must be a dictionary")
            return False, errors
        
        all_tags = self.get_all_tags()
        basic_keys = list(self.others.keys())
        
        for path, values in selections.items():
            # Check if path exists
            if path not in all_tags and path not in basic_keys:
                errors.append(f"Unknown filter path: {path}")
            
            # Check values
            if not isinstance(values, (list, str, int)):
                errors.append(f"Values for {path} must be list, string, or number")
        
        return len(errors) == 0, errors
    
    def get_synonyms(self, tag_path: str) -> List[str]:
        """
        Get all synonyms for a tag
        
        Args:
            tag_path: Tag path (e.g., 'intervention.vpd.covid')
        
        Returns: List of synonyms
        """
        try:
            all_tags = self.get_all_tags()
            return all_tags.get(tag_path, {}).get('synonyms', [])
        except Exception as e:
            self.logger.error(f"Error getting synonyms for {tag_path}: {str(e)}")
            return []
    
    def expand_synonym(self, synonym: str) -> Optional[str]:
        """
        Find tag path for a synonym
        
        Args:
            synonym: Synonym to find
        
        Returns: Tag path or None
        """
        try:
            all_tags = self.get_all_tags()
            
            for path, tag_info in all_tags.items():
                if synonym in tag_info.get('synonyms', []):
                    return path
            
            return None
        except Exception as e:
            self.logger.error(f"Error expanding synonym '{synonym}': {str(e)}")
            return None