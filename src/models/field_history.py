"""
Field history models for the BDA optimization application.
"""
from typing import List, Optional
from pydantic import BaseModel, Field

class FieldHistory(BaseModel):
    """
    Tracks the history of instructions, results, and similarities for a field.
    """
    field_name: str = Field(description="The name of the field")
    instructions: List[str] = Field(default_factory=list, description="History of instructions")
    results: List[str] = Field(default_factory=list, description="History of results")
    similarities: List[float] = Field(default_factory=list, description="History of similarity scores")
    
    def add_attempt(self, instruction: str, result: str, similarity: float) -> None:
        """
        Add an attempt to the history.
        
        Args:
            instruction: Instruction used
            result: Result obtained
            similarity: Similarity score
            
        Raises:
            ValueError: If parameters are invalid
        """
        # Validate input parameters
        if instruction is None or not isinstance(instruction, str):
            raise ValueError("instruction must be a non-null string")
        if result is None or not isinstance(result, str):
            raise ValueError("result must be a non-null string")
        if similarity is None or not isinstance(similarity, (int, float)):
            raise ValueError("similarity must be a numeric value")
        if not (0.0 <= float(similarity) <= 1.0):
            raise ValueError("similarity must be between 0.0 and 1.0")
        
        self.instructions.append(instruction)
        self.results.append(result)
        self.similarities.append(float(similarity))
    
    def get_best_instruction(self) -> Optional[str]:
        """
        Get the instruction with the highest similarity score.
        
        Returns:
            str or None: Best instruction, or None if no attempts
        """
        if not self.similarities:
            return None
        
        # Find index of highest similarity
        best_index = self.similarities.index(max(self.similarities))
        
        return self.instructions[best_index]
    
    def get_last_instruction(self) -> Optional[str]:
        """
        Get the most recent instruction.
        
        Returns:
            str or None: Last instruction, or None if no attempts
        """
        if not self.instructions:
            return None
        
        return self.instructions[-1]
    
    def get_all_attempts(self) -> List[dict]:
        """
        Get all attempts as a list of dictionaries.
        
        Returns:
            List[dict]: List of attempts
        """
        attempts = []
        for i, (instruction, result, similarity) in enumerate(zip(self.instructions, self.results, self.similarities)):
            attempts.append({
                "attempt": i + 1,
                "instruction": instruction,
                "result": result,
                "similarity": similarity
            })
        return attempts

class FieldHistoryManager(BaseModel):
    """
    Manages field histories for all fields.
    """
    histories: dict[str, FieldHistory] = Field(default_factory=dict, description="Field histories by field name")
    
    def initialize(self, field_names: List[str]) -> None:
        """
        Initialize histories for fields.
        
        Args:
            field_names: List of field names
            
        Raises:
            ValueError: If field_names is None or empty
        """
        # Input validation for field_names parameter
        if field_names is None:
            raise ValueError("field_names cannot be None")
        if not isinstance(field_names, list):
            raise ValueError("field_names must be a list")
        if len(field_names) == 0:
            raise ValueError("field_names cannot be empty")
        
        for field_name in field_names:
            if field_name not in self.histories:
                self.histories[field_name] = FieldHistory(field_name=field_name)
    
    def add_attempt(self, field_name: str, instruction: str, result: str, similarity: float) -> None:
        """
        Add an attempt for a field.
        
        Args:
            field_name: Name of the field
            instruction: Instruction used
            result: Result obtained
            similarity: Similarity score
        """
        if field_name not in self.histories:
            self.histories[field_name] = FieldHistory(field_name=field_name)
        
        self.histories[field_name].add_attempt(instruction, result, similarity)
    
    def get_best_instruction(self, field_name: str) -> Optional[str]:
        """
        Get the best instruction for a field.
        
        Args:
            field_name: Name of the field
            
        Returns:
            str or None: Best instruction, or None if no attempts
        """
        if field_name not in self.histories:
            return None
        
        return self.histories[field_name].get_best_instruction()
    
    def get_field_history(self, field_name: str) -> Optional[FieldHistory]:
        """
        Get the history for a field.
        
        Args:
            field_name: Name of the field
            
        Returns:
            FieldHistory or None: Field history, or None if not found
        """
        return self.histories.get(field_name)
