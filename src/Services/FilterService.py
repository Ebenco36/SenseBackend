"""
FilterService - Fully aligned with your actual database structure
Handles __hash__ hierarchical columns perfectly
Extracts simple filters (country, language, year, etc.)
All filters include column mappings
Ready for production
"""

import ast
from typing import Dict, List, Any, Optional
from collections import defaultdict
from sqlalchemy import inspect
from database.db import db
import logging

from src.Utils.filter_structure import FILTER_STRUCTURE

logger = logging.getLogger(__name__)
DEFAULT_FILTER_STRUCTURE = FILTER_STRUCTURE

class FilterService:
    """
    Service for extracting and managing filters from database.
    Perfectly aligned with:
    - Hierarchical columns: popu__hash__age__group__hash__nb_0__1
    - Simple columns: country, language, year, region
    - Includes column mappings in all responses
    """
    
    _cache: Optional[Dict[str, Any]] = None
    _cache_version: int = 0
    
    def __init__(self, registry=None):
        """Initialize with registry for model lookup."""
        self.registry = registry
        self.logger = logger

    def get_all_filters(self, table_name: str = "all_db", bypass_cache: bool = False) -> Dict[str, Any]:
        """Get all filters from database or use defaults with column mappings."""
        try:
            if not bypass_cache and self._cache:
                self.logger.debug("Using cached filters")
                return self._cache
            
            self.logger.info(f"Loading filters from {table_name}")
            model_class = self._get_model_from_registry(table_name)
            if not model_class:
                self.logger.warning("Model not found, using DEFAULT_FILTER_STRUCTURE")
                self._cache = DEFAULT_FILTER_STRUCTURE
                return self._cache

            # Extract from database
            others = self._extract_simple_filters(model_class)
            tag_filters = self._extract_hierarchical_filters(model_class)
            
            # Fallback to defaults if not found
            if not tag_filters:
                tag_filters = DEFAULT_FILTER_STRUCTURE.get("tag_filters", {})
            if not others:
                others = DEFAULT_FILTER_STRUCTURE.get("others", {})

            result = {
                "others": others,
                "tag_filters": tag_filters
            }
            self._cache = result
            self._cache_version += 1
            self.logger.info("Filters loaded successfully")
            return result
        
        except Exception as e:
            self.logger.error(f"Error loading filters: {str(e)}", exc_info=True)
            return DEFAULT_FILTER_STRUCTURE

    def _get_model_from_registry(self, table_name: str):
        """Get model from registry by name."""
        if not self.registry:
            self.logger.error("Registry is None")
            return None
        if table_name in self.registry.models:
            return self.registry.models[table_name]
        
        normalized_search = self._normalize_name(table_name)
        for key, model in self.registry.models.items():
            if self._normalize_name(key) == normalized_search:
                self.logger.info(f"Found model: {key}")
                return model
        
        available = list(self.registry.models.keys())
        self.logger.warning(f"Model not found. Available: {available}")
        return None

    def _normalize_name(self, name: str) -> str:
        """Normalize name for comparison."""
        return name.replace('_', '').lower()


    def _extract_simple_filters(self, model_class) -> Dict[str, Any]:
        """
        Extract simple filters (country, language, year, region, amstar_label)
        Returns format: { "Country": {"column": "country", "values": [...]} }
        Handles region columns with dict-like structures
        """
        import ast
        import json
        import re
        
        try:
            others = {}
            
            # Map display name to database column
            simple_filters = {
                'AMSTAR 2 Rating': 'amstar_label',
                'Country': 'country',
                'Language': 'language',
                'Region': 'study_country__hash__countries__hash__region',
                'Year': 'year'
            }
            
            mapper = inspect(model_class)
            available_columns = {col.name for col in mapper.columns}
            
            self.logger.info(f"Available columns in model: {available_columns}")
            
            def unwrap_value(v):
                """
                Recursively unwrap SQLAlchemy result tuples
                v could be: (value,) or ((value,),) or value
                """
                while isinstance(v, (tuple, list)) and len(v) == 1:
                    v = v[0]
                return v
            
            def parse_region_value(v):
                """
                Parse region value from various formats:
                - Dict: {'Africa': True, 'Asia': True}
                - String: "{'Africa': True, 'Asia': True}"
                - Tuple: ("{'Africa': True}",)
                Returns set of region names where value is True
                """
                if len(v) > 0:
                    v = ast.literal_eval(v[0])
                regions = set()
                
                if v is None:
                    return regions
                
                # First, unwrap any tuples
                v = unwrap_value(v)
                
                if v is None:
                    return regions
                
                # Case 1: Already a dict
                if isinstance(v, dict):
                    for region_name, is_active in v.items():
                        if is_active is True or str(is_active).lower() == 'true':
                            clean_name = str(region_name).strip()
                            if clean_name and clean_name.lower() not in ['none', 'null', '']:
                                regions.add(clean_name)
                    return regions
                
                # Case 2: String - try multiple parsing strategies
                if isinstance(v, str):
                    current = v.strip()
                    
                    # Strategy 1: Multi-level unwrapping and parsing
                    for _ in range(5):  # Try up to 5 levels of unwrapping
                        # Remove outer quotes
                        if len(current) >= 2:
                            if (current[0] == '"' and current[-1] == '"') or \
                            (current[0] == "'" and current[-1] == "'"):
                                current = current[1:-1].strip()
                        
                        # Try to parse with ast.literal_eval
                        if current.startswith('{') and '}' in current:
                            try:
                                parsed = ast.literal_eval(current)
                                if isinstance(parsed, dict):
                                    # Successfully parsed as dict
                                    for region_name, is_active in parsed.items():
                                        if is_active is True or str(is_active).lower() == 'true':
                                            clean_name = str(region_name).strip()
                                            if clean_name and clean_name.lower() not in ['none', 'null', '']:
                                                regions.add(clean_name)
                                    return regions
                                elif isinstance(parsed, str):
                                    # Parsed to another string, continue unwrapping
                                    current = parsed.strip()
                                    continue
                                else:
                                    break
                            except (ValueError, SyntaxError):
                                pass
                        else:
                            break
                    
                    # Strategy 2: JSON parsing (replace single quotes with double)
                    try:
                        json_str = current.replace("'", '"')
                        parsed = json.loads(json_str)
                        if isinstance(parsed, dict):
                            for region_name, is_active in parsed.items():
                                if is_active is True or str(is_active).lower() == 'true':
                                    clean_name = str(region_name).strip()
                                    if clean_name and clean_name.lower() not in ['none', 'null', '']:
                                        regions.add(clean_name)
                            return regions
                    except (json.JSONDecodeError, ValueError):
                        pass
                    
                    # Strategy 3: Regex extraction as last resort
                    # Extract all strings followed by ': True'
                    pattern = r"['\"]([^'\"]+)['\"]:\s*True"
                    matches = re.findall(pattern, current)
                    if matches:
                        for region_name in matches:
                            clean_name = region_name.strip()
                            if clean_name and clean_name.lower() not in ['none', 'null', '']:
                                regions.add(clean_name)
                        return regions
                
                return regions
            
            # ---- Extract values from DB for each simple filter ----
            for display_name, column_name in simple_filters.items():
                try:
                    if column_name not in available_columns:
                        self.logger.debug(f"Column {column_name} not found, skipping {display_name}")
                        continue
                    
                    column = getattr(model_class, column_name)
                    
                    # Distinct non-null values
                    values = db.session.query(column).distinct().filter(
                        column.isnot(None)
                    ).all()
                    
                    extracted = set()
                    
                    # Special handling for Region
                    if display_name == 'Region':
                        self.logger.info(f"Processing {len(values)} Region rows")
                        
                        for idx, row_value in enumerate(values):
                            # Unwrap SQLAlchemy result
                            v = unwrap_value(row_value)
                            
                            if v is None:
                                continue
                            
                            # Debug: log the actual value type and content
                            self.logger.debug(f"Row {idx+1}: type={type(v)}, value={str(v)[:100]}")
                            
                            # Parse and extract regions
                            regions = parse_region_value(v)
                            print(regions)
                            if regions:
                                extracted.update(regions)
                                self.logger.debug(f"Row {idx+1}: extracted {len(regions)} regions: {regions}")
                            else:
                                self.logger.warning(f"Row {idx+1}: no regions extracted from value: {v}")
                        
                        self.logger.info(f"Extracted {len(extracted)} unique regions: {sorted(extracted)}")
                        
                        if extracted:
                            sorted_values = sorted(list(extracted))
                            others[display_name] = {
                                "column": column_name,
                                "values": list(set(sorted_values))
                            }
                        continue
                    
                    # Normal processing for other filters
                    for row_value in values:
                        v = unwrap_value(row_value)
                        
                        if v is None:
                            continue
                        
                        cleaned = self._ultra_clean_value(str(v))
                        
                        # Split by comma for Country, Language
                        if display_name in ['Language', 'Country']:
                            for item in cleaned.split(','):
                                item_clean = item.strip()
                                if item_clean and item_clean not in ['', '[]', 'None', 'null']:
                                    extracted.add(item_clean)
                        else:
                            if cleaned and cleaned not in ['', '[]', 'None', 'null']:
                                extracted.add(cleaned)
                    
                    if extracted:
                        sorted_values = self._sort_values(list(extracted), display_name)
                        # print(list(set(sorted_values)))
                        others[display_name] = {
                            "column": column_name,
                            "values": list(set(sorted_values))
                        }
                        self.logger.info(f"{display_name}: {len(sorted_values)} values extracted")
                    else:
                        self.logger.debug(f"{display_name}: No values found in database")
                
                except Exception as e:
                    self.logger.warning(f"Could not extract {display_name}: {e}", exc_info=True)
                    continue
            
            # ---- Merge / enrich with DEFAULT_FILTER_STRUCTURE["others"] ----
            default_others = DEFAULT_FILTER_STRUCTURE.get("others", {}) or {}
            
            for filter_name, default_data in default_others.items():
                if not isinstance(default_data, dict) or "values" not in default_data:
                    continue
                
                manual_values = set(default_data.get("values", []))
                manual_column = default_data.get(
                    "column",
                    simple_filters.get(filter_name, filter_name.lower())
                )
                
                if filter_name in others:
                    # Merge DB values with manual extras
                    existing_values = set(others[filter_name].get("values", []))
                    merged = existing_values.union(manual_values)
                    others[filter_name]["values"] = list(set(self._sort_values(list(merged), filter_name)))
                    if manual_column:
                        others[filter_name]["column"] = manual_column
                    self.logger.info(
                        f"Merged manual defaults into {filter_name} "
                        f"(total: {len(others[filter_name]['values'])})"
                    )
                else:
                    # Only manual/default values for this filter
                    others[filter_name] = {
                        "column": manual_column,
                        "values": list(set(self._sort_values(list(manual_values), filter_name)))
                    }
                    self.logger.info(f"Using default values only for {filter_name}")
            
            return others
        
        except Exception as e:
            self.logger.error(f"Error extracting simple filters: {e}", exc_info=True)
            return {}



        
    def _extract_hierarchical_filters(self, model_class) -> Dict[str, Any]:
        """
        Extract hierarchical filters using __hash__ separator
        Handles columns like: popu__hash__age__group__hash__nb_0__1
        Returns format with column mappings at all levels
        """
        try:
            hierarchical = defaultdict(lambda: defaultdict(dict))
            mapper = inspect(model_class)
            columns = [col.name for col in mapper.columns]
            
            # Find all hierarchical columns (contain __hash__)
            tag_columns = [c for c in columns if "__hash__" in c]
            
            self.logger.info(f"Found {len(tag_columns)} hierarchical columns")
            
            if not tag_columns:
                self.logger.warning("❌ NO hierarchical columns found!")
                return {}
            
            # Parse each hierarchical column
            for column_name in tag_columns:
                try:
                    # Split by __hash__ separator
                    # Example: popu__hash__age__group__hash__nb_0__1
                    # Becomes: [popu, age__group, nb_0__1]
                    parts = column_name.split("__hash__")
                    
                    if len(parts) < 2:
                        self.logger.warning(f"Invalid format: {column_name}")
                        continue
                    
                    category = parts  # e.g., "popu"
                    subgroup = parts if len(parts) > 1 else category  # e.g., "age__group"
                    item_key = parts if len(parts) > 2 else parts  # e.g., "nb_0__1"
                    
                    self.logger.debug(f"Processing: category={category}, subgroup={subgroup}, item={item_key}")
                    
                    # Query values from database
                    column = getattr(model_class, column_name)
                    values = db.session.query(column).distinct().filter(
                        column.isnot(None)
                    ).all()
                    
                    self.logger.debug(f"Column {column_name}: {len(values)} distinct values")
                    
                    # Store each value
                    for value_tuple in values:
                        value = value_tuple if isinstance(value_tuple, tuple) else value_tuple
                        if value is not None:
                            val_str = self._ultra_clean_value(str(value))
                            
                            if val_str and val_str not in ['', 'None', 'nan']:
                                hierarchical[category][subgroup][item_key] = {
                                    "display": val_str,
                                    "synonyms": [val_str],
                                    "additional_context": "None",
                                    "column": column_name  # Include column mapping
                                }
                
                except Exception as e:
                    self.logger.warning(f"Error processing {column_name}: {e}")
                    continue
            
            # Convert defaultdict to regular dict
            result = {}
            for category, subgroups in hierarchical.items():
                result[category] = dict(subgroups)
                # Add parent level column mapping
                for category_key in DEFAULT_FILTER_STRUCTURE.get("tag_filters", {}).keys():
                    if category_key == category:
                        result[category]["column"] = DEFAULT_FILTER_STRUCTURE["tag_filters"][category_key].get("column", category)
                        self.logger.debug(f"Added parent column for {category}")
            
            self.logger.info(f"Extracted {len(result)} hierarchical categories")
            return result
        
        except Exception as e:
            self.logger.error(f"Error extracting hierarchical filters: {e}", exc_info=True)
            return {}

    def _ultra_clean_value(self, value: str) -> str:
        """
        Ultra aggressive cleaning for string representations.
        Handles nested parentheses, brackets, quotes, etc.
        """
        try:
            cleaned = value.strip()
            if not cleaned:
                return ""
            
            # Multi-pass cleaning (up to 5 levels)
            for _ in range(5):
                old = cleaned
                
                # Remove parentheses with trailing comma
                while cleaned.startswith('(') and cleaned.endswith(',)'):
                    cleaned = cleaned[1:-2].strip()
                while cleaned.startswith('(') and cleaned.endswith(')'):
                    cleaned = cleaned[1:-1].strip()
                
                # Remove square brackets
                while cleaned.startswith('[') and cleaned.endswith(']'):
                    cleaned = cleaned[1:-1].strip()
                
                # Remove quotes
                prev = None
                while prev != cleaned:
                    prev = cleaned
                    if cleaned.startswith('"') and cleaned.endswith('"'):
                        cleaned = cleaned[1:-1].strip()
                    elif cleaned.startswith("'") and cleaned.endswith("'"):
                        cleaned = cleaned[1:-1].strip()
                
                if old == cleaned:
                    break
            
            # Handle empty values
            if cleaned in ['[]', '', 'None', 'null', 'nan']:
                return ""
            
            return cleaned.strip()
        
        except Exception as e:
            self.logger.warning(f"Error ultra-cleaning: {e}")
            return ""

    def _sort_values(self, values: List[Any], filter_name: str = None) -> List[Any]:
        """
        Sort values intelligently.
        Year returns integers [2026, 2025, ...]
        Other filters return sorted strings
        """
        if not values:
            return []
        
        # Special handling for Year - return as integers sorted descending
        if filter_name == 'Year':
            try:
                numeric_values = []
                for v in values:
                    try:
                        numeric_values.append((float(str(v).strip()), v))
                    except (TypeError, ValueError):
                        self.logger.warning(f"Could not convert {v} to float")
                        break
                
                # All values are numeric
                if len(numeric_values) == len(values):
                    result = [int(v) for v in sorted(numeric_values, reverse=True)]
                    self.logger.debug(f"Year values sorted: {result}")
                    return result
            except Exception as e:
                self.logger.warning(f"Error sorting Year numerically: {e}")
        
        # For other filters: alphabetical sort
        try:
            result = sorted([str(v).strip() for v in values])
            return result
        except Exception as e:
            self.logger.warning(f"Error sorting values: {e}")
            return list(values)

    def get_filter_by_name(self, filter_name: str, table_name: str = "all_db") -> Optional[Dict]:
        """Get specific filter by name."""
        try:
            all_filters = self.get_all_filters(table_name)
            
            # Check in simple filters
            if filter_name in all_filters['others']:
                return {
                    'name': filter_name,
                    'type': 'simple',
                    'column': all_filters['others'][filter_name].get("column"),
                    'values': all_filters['others'][filter_name].get("values")
                }
            
            # Check in hierarchical filters
            for category, subgroups in all_filters['tag_filters'].items():
                if filter_name == category:
                    return {
                        'name': filter_name,
                        'type': 'hierarchical',
                        'data': subgroups,
                        'column': subgroups.get("column")
                    }
            
            return None
        
        except Exception as e:
            self.logger.error(f"Error getting filter: {e}")
            return None

    def invalidate_cache(self):
        """Clear the filter cache."""
        self._cache = None
        self._cache_version += 1
        self.logger.info("Filter cache invalidated")

    def get_columns_map(self, table_name: str = "all_db") -> Dict[str, str]:
        """
        Get mapping of all filter names to their database columns.
        Useful for building queries.
        """
        try:
            all_filters = self.get_all_filters(table_name)
            column_map = {}
            
            # Simple filters
            for filter_name, filter_data in all_filters.get("others", {}).items():
                if isinstance(filter_data, dict):
                    column_map[filter_name] = filter_data.get("column", filter_name.lower())
            
            # Hierarchical filters
            for category, subgroups in all_filters.get("tag_filters", {}).items():
                for subgroup_name, items in subgroups.items():
                    if subgroup_name == "column":
                        continue
                    if isinstance(items, dict):
                        for item_name, item_data in items.items():
                            if item_name == "column":
                                continue
                            if isinstance(item_data, dict):
                                display = item_data.get("display", item_name)
                                column = item_data.get("column", f"{category}__{item_name}")
                                column_map[display] = column
            
            self.logger.info(f"Generated column map with {len(column_map)} entries")
            return column_map
        
        except Exception as e:
            self.logger.error(f"Error generating column map: {e}")
            return {}