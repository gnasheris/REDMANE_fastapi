from pydantic import BaseModel
from typing import List, Optional

# Pydantic model for Project
class Project(BaseModel):
    id: int
    name: str
    status: str

# Pydantic model for Dataset
class Dataset(BaseModel):
    id: int
    project_id: int
    name: str

class DatasetMetadata(BaseModel):
    id: int
    dataset_id: int
    key: str
    value: str

class DatasetWithMetadata(Dataset):
    metadata: List[DatasetMetadata] = []

# Pydantic model for Patient
class Patient(BaseModel):
    id: int
    project_id: int
    ext_patient_id: str
    ext_patient_url: str
    public_patient_id: Optional[str]

# Pydantic model for Patient with sample count
class PatientWithSampleCount(Patient):
    sample_count: int

# Pydantic model for PatientMetadata
class PatientMetadata(BaseModel):
    id: int
    patient_id: int
    key: str
    value: str

# Pydantic model for Patient with Metadata
class PatientWithMetadata(Patient):
    metadata: List[PatientMetadata] = []

# Pydantic model for SampleMetadata
class SampleMetadata(BaseModel):
    id: int
    sample_id: int
    key: str
    value: str

# Pydantic model for Sample
class Sample(BaseModel):
    id: int
    patient_id: int
    ext_sample_id: str
    ext_sample_url: str
    metadata: List[SampleMetadata] = []
    patient: Patient

# Pydantic model for SampleWithoutPatient
class SampleWithoutPatient(BaseModel):
    id: int
    patient_id: int
    ext_sample_id: str
    ext_sample_url: str
    metadata: List[SampleMetadata] = []

class RawFileResponse(BaseModel):
    id: int
    path: str
    sample_id: Optional[str] = None
    ext_sample_id: Optional[str] = None
    sample_metadata: Optional[List[SampleMetadata]] = None

# Pydantic model for Patient with Samples
class PatientWithSamples(PatientWithMetadata):
    samples: List[SampleWithoutPatient] = []

# Pydantic model for RawFileMetadata
class RawFileMetadataCreate(BaseModel):
    metadata_key: str
    metadata_value: str

# Updated Pydantic model for RawFile with nested metadata
class RawFileCreate(BaseModel):
    dataset_id: int
    path: str
    metadata: Optional[List[RawFileMetadataCreate]] = []

class MetadataUpdate(BaseModel):
    dataset_id: int
    raw_file_size: str
    last_size_update: str