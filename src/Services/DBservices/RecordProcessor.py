# src/Services/DBservices/RecordProcessor.py
"""
Dynamic RecordProcessor with automatic field discovery.
FIXED: Only processes __hash__ fields, excludes dates and other metadata.
"""
import json
import logging
from typing import List, Dict, Any, Set

logger = logging.getLogger(__name__)


class RecordProcessor:
    """
    Dynamically discovers and processes all __hash__ fields in records.
    Excludes metadata fields (dates, IDs, titles, etc.)
    """
    
    # Fields to EXCLUDE from artificial columns
    EXCLUDED_FIELDS = {
        'primary_id', 'title', 'authors', 'year', 'doi', 'pmid',
        'lit_search_date', 'search_year_start', 'search_year_end',
        'publication_date', 'total_study_count', 'total_rct_count',
        'total_cohort_count', 'total_case_control_count', 'total_participants',
        'amstar_score', 'amstar_label', 'risk_of_bias', 'open_access',
        'extraction_timestamp', 'extraction_confidence', 'created_at', 'updated_at'
    }
    
    # GROUPING RULES (which patterns go into which artificial column)
    GROUPING = {
        'research_notes': [
            'intervention__hash__vpd__hash',  # All VPD interventions
            'topic__hash__coverage__hash',     # Coverage topics
        ],
        'topic_notes': [
            'topic__hash__',  # All topic fields
        ],
        'notes': [
            'outcome__hash__',  # Outcomes only
        ]
    }
    
    def __init__(self, include_empty: bool = False):
        """Initialize processor."""
        self.logger = logger
        self.include_empty = include_empty
        self._field_cache: Dict[str, Set[str]] = {}
    
    def add_artificial_columns(self, records: List[Dict]) -> List[Dict]:
        """Add artificial columns by dynamically discovering __hash__ fields only."""
        if not records:
            return []
        
        # Discover fields from first record (cache for batch)
        batch_id = id(records)
        if batch_id not in self._field_cache:
            self._discover_fields(records[0])
            self._field_cache[batch_id] = True
        
        for record in records:
            if not record:
                continue
            
            try:
                # Dynamically extract for each group
                research_notes = self._extract_for_group(record, 'research_notes')
                topic_notes = self._extract_for_group(record, 'topic_notes')
                notes = self._extract_for_group(record, 'notes')
                
                # Add columns (only if content or include_empty=True)
                if self.include_empty or research_notes:
                    record["research_notes"] = research_notes
                
                if self.include_empty or topic_notes:
                    record["topic_notes"] = topic_notes
                
                if self.include_empty or notes:
                    record["notes"] = notes
            
            except Exception as e:
                self.logger.warning(
                    f"Error processing record {record.get('primary_id', 'unknown')}: {e}"
                )
                if self.include_empty:
                    record.setdefault("research_notes", "")
                    record.setdefault("notes", "")
                    record.setdefault("topic_notes", "")
        
        return records
    
    def _discover_fields(self, sample_record: Dict) -> Dict[str, List[str]]:
        """
     FIXED: Discover ONLY __hash__ fields, exclude metadata.
        """
        discovered = {
            'research_notes': [],
            'topic_notes': [],
            'notes': []
        }
        
        # Scan all fields in record
        for field_name in sample_record.keys():
            # CRITICAL FIX: Skip if field is in EXCLUDED_FIELDS
            if field_name in self.EXCLUDED_FIELDS:
                continue
            
            # CRITICAL FIX: Skip if field doesn't contain __hash__
            if '__hash__' not in field_name:
                continue
            
            # Categorize by matching patterns
            categorized = False
            
            # Check research_notes patterns
            for pattern in self.GROUPING['research_notes']:
                if field_name.startswith(pattern):
                    discovered['research_notes'].append(field_name)
                    categorized = True
                    break
            
            if categorized:
                continue
            
            # Check topic_notes patterns
            for pattern in self.GROUPING['topic_notes']:
                if field_name.startswith(pattern):
                    discovered['topic_notes'].append(field_name)
                    categorized = True
                    break
            
            if categorized:
                continue
            
            # Check notes patterns (don't auto-add everything)
            for pattern in self.GROUPING['notes']:
                if field_name.startswith(pattern):
                    discovered['notes'].append(field_name)
                    categorized = True
                    break
            
            # CRITICAL FIX: If still not categorized, log but DON'T add to notes
            if not categorized:
                self.logger.debug(
                    f"Uncategorized __hash__ field (not added): {field_name}"
                )
        
        self.logger.info(
            f"Discovered fields - "
            f"research_notes: {len(discovered['research_notes'])}, "
            f"topic_notes: {len(discovered['topic_notes'])}, "
            f"notes: {len(discovered['notes'])}"
        )
        
        # Cache discovered fields
        self._discovered_fields = discovered
        return discovered
    
    def _extract_for_group(self, record: Dict, group_name: str) -> str:
        """Extract and format values for a specific group."""
        if not hasattr(self, '_discovered_fields'):
            self._discover_fields(record)
        
        fields = self._discovered_fields.get(group_name, [])
        values = []
        
        for field in fields:
            if field not in record or not record[field]:
                continue
            
            field_value = record[field]
            extracted = self._parse_field_value(field_value)
            values.extend(extracted)
        
        # Remove duplicates and sort
        unique_values = sorted(list(set(filter(None, values))))
        return ", ".join(unique_values)
    
    def _parse_field_value(self, field_value: Any) -> List[str]:
        """Parse field value and extract tag codes."""
        values = []
        
        # Handle None/empty
        if not field_value:
            return []
        
        # Format 1: Dictionary {"covid": True}
        if isinstance(field_value, dict):
            for key, val in field_value.items():
                if val is True:
                    tag = key.split(":")[-1].strip()
                    if tag:
                        values.append(tag)
            return values
        
        # Format 2: String
        if isinstance(field_value, str):
            # Try JSON parsing first
            try:
                parsed = json.loads(field_value.replace("'", '"'))
                if isinstance(parsed, dict):
                    for key, val in parsed.items():
                        if val is True:
                            tag = key.split(":")[-1].strip()
                            if tag:
                                values.append(tag)
                    return values
            except:
                pass
            
            # YOUR FORMAT: "influenza:infl" or "covid:covid,hpv:hpv"
            parts = field_value.split(",")
            
            for part in parts:
                part = part.strip()
                if not part:
                    continue
                
                # Extract tag code (part after last colon)
                if ":" in part:
                    tag = part.split(":")[-1].strip()
                    if tag:
                        values.append(tag)
                else:
                    # No colon, use whole value
                    values.append(part)
        
        return values
    
    def get_discovered_fields(self) -> Dict[str, List[str]]:
        """Get discovered fields for debugging."""
        if hasattr(self, '_discovered_fields'):
            return self._discovered_fields
        return {}
