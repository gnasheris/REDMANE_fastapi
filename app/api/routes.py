from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import RedirectResponse

import psycopg2
from psycopg2 import Error

from app.schemas.schemas import (
    RawFileCreate,
    Project,
    Dataset,
    DatasetMetadata,
    DatasetWithMetadata,
    Patient,
    PatientWithSampleCount,
    PatientMetadata,
    PatientWithMetadata,
    SampleMetadata,
    Sample,
    SampleWithoutPatient,
    RawFileResponse,
    PatientWithSamples,
    RawFileMetadataCreate,
    MetadataUpdate,
)

# Replace with your actual connection details
DB_NAME = "readmedatabase"
DB_USER = "username"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"

router = APIRouter()


def get_connection():
    """
    Helper function to get a new connection to your PostgreSQL database.
    """
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )


@router.post("/add_raw_files/")
async def add_raw_files(raw_files: List[RawFileCreate]):
    """
    Insert new raw_files (and associated metadata) into the DB.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        raw_file_ids = []
        for raw_file in raw_files:
            # Use RETURNING id to get the auto-incremented ID in PostgreSQL
            cursor.execute(
                """
                INSERT INTO raw_files (dataset_id, path)
                VALUES (%s, %s)
                RETURNING id
                """,
                (raw_file.dataset_id, raw_file.path)
            )
            raw_file_id = cursor.fetchone()[0]
            raw_file_ids.append(raw_file_id)

            # Insert associated metadata
            if raw_file.metadata:
                for metadata in raw_file.metadata:
                    cursor.execute(
                        """
                        INSERT INTO raw_files_metadata (raw_file_id, metadata_key, metadata_value)
                        VALUES (%s, %s, %s)
                        """,
                        (raw_file_id, metadata.metadata_key, metadata.metadata_value)
                    )

        conn.commit()
        conn.close()
        return {"status": "success", "message": "Raw files and metadata added successfully"}

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/")
async def root():
    return RedirectResponse(url="/projects")


@router.get("/patients_metadata/{patient_id}", response_model=List[PatientWithSamples])
async def get_patients_metadata(project_id: int, patient_id: int):
    """
    Fetch patients (and their samples + metadata) for a given project_id.
    If patient_id == 0, fetch all patients; otherwise, fetch the specified patient.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        if patient_id != 0:
            cursor.execute(
                """
                SELECT p.id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id,
                       pm.id, pm.key, pm.value
                FROM patients p
                LEFT JOIN patients_metadata pm ON p.id = pm.patient_id
                WHERE p.project_id = %s AND p.id = %s
                ORDER BY p.id
                """,
                (project_id, patient_id)
            )
        else:
            cursor.execute(
                """
                SELECT p.id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id,
                       pm.id, pm.key, pm.value
                FROM patients p
                LEFT JOIN patients_metadata pm ON p.id = pm.patient_id
                WHERE p.project_id = %s
                ORDER BY p.id
                """,
                (project_id,)
            )

        rows = cursor.fetchall()

        patients = []
        current_patient = None
        for row in rows:
            # row = [p.id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id,
            #        pm.id, pm.key, pm.value]
            if not current_patient or current_patient['id'] != row[0]:
                if current_patient:
                    patients.append(current_patient)
                current_patient = {
                    'id': row[0],
                    'project_id': row[1],
                    'ext_patient_id': row[2],
                    'ext_patient_url': row[3],
                    'public_patient_id': row[4],
                    'samples': [],
                    'metadata': []
                }

            if row[5]:  # pm.id is not None
                current_patient['metadata'].append({
                    'id': row[5],
                    'patient_id': row[0],
                    'key': row[6],
                    'value': row[7]
                })

        if current_patient:
            patients.append(current_patient)

        # Now fetch samples for each patient
        for patient in patients:
            cursor.execute(
                """
                SELECT s.id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                       sm.id, sm.key, sm.value
                FROM samples s
                LEFT JOIN samples_metadata sm ON s.id = sm.sample_id
                WHERE s.patient_id = %s
                ORDER BY s.id
                """,
                (patient['id'],)
            )

            sample_rows = cursor.fetchall()
            current_sample = None
            for sample_row in sample_rows:
                # sample_row = [s.id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                #               sm.id, sm.key, sm.value]
                if not current_sample or current_sample['id'] != sample_row[0]:
                    if current_sample:
                        patient['samples'].append(current_sample)
                    current_sample = {
                        'id': sample_row[0],
                        'patient_id': sample_row[1],
                        'ext_sample_id': sample_row[2],
                        'ext_sample_url': sample_row[3],
                        'metadata': []
                    }
                if sample_row[4]:  # sm.id is not None
                    current_sample['metadata'].append({
                        'id': sample_row[4],
                        'sample_id': sample_row[0],
                        'key': sample_row[5],
                        'value': sample_row[6]
                    })

            if current_sample:
                patient['samples'].append(current_sample)

        conn.close()
        return patients

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/samples/{sample_id}", response_model=List[Sample])
async def get_samples_per_patient(sample_id: int, project_id: int):
    """
    Fetch samples (and their metadata) for a given project_id, optionally filtering by sample_id.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        if sample_id != 0:
            cursor.execute(
                """
                SELECT s.id AS sample_id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                       sm.id AS metadata_id, sm.key, sm.value,
                       p.id AS patient_id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id
                FROM samples s
                LEFT JOIN samples_metadata sm ON s.id = sm.sample_id
                LEFT JOIN patients p ON s.patient_id = p.id
                WHERE p.project_id = %s AND s.id = %s
                ORDER BY s.id, sm.id
                """,
                (project_id, sample_id)
            )
        else:
            cursor.execute(
                """
                SELECT s.id AS sample_id, s.patient_id, s.ext_sample_id, s.ext_sample_url,
                       sm.id AS metadata_id, sm.key, sm.value,
                       p.id AS patient_id, p.project_id, p.ext_patient_id, p.ext_patient_url, p.public_patient_id
                FROM samples s
                LEFT JOIN samples_metadata sm ON s.id = sm.sample_id
                LEFT JOIN patients p ON s.patient_id = p.id
                WHERE p.project_id = %s
                ORDER BY s.id, sm.id
                """,
                (project_id,)
            )

        rows = cursor.fetchall()
        conn.close()

        samples = []
        current_sample = None

        for row in rows:
            # row = [sample_id, patient_id, ext_sample_id, ext_sample_url,
            #        metadata_id, key, value, pat_id, project_id, ext_pat_id, ext_pat_url, public_pat_id]
            if not current_sample or current_sample['id'] != row[0]:
                if current_sample:
                    samples.append(current_sample)
                current_sample = {
                    'id': row[0],
                    'patient_id': row[1],
                    'ext_sample_id': row[2],
                    'ext_sample_url': row[3],
                    'metadata': [],
                    'patient': {
                        'id': row[7],
                        'project_id': row[8],
                        'ext_patient_id': row[9],
                        'ext_patient_url': row[10],
                        'public_patient_id': row[11]
                    }
                }

            if row[4]:  # metadata_id is not None
                current_sample['metadata'].append({
                    'id': row[4],
                    'sample_id': row[0],
                    'key': row[5],
                    'value': row[6]
                })

        if current_sample:
            samples.append(current_sample)

        return samples

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/patients/", response_model=List[PatientWithSampleCount])
async def get_patients(
        project_id: Optional[int] = Query(None, description="Filter by project ID")
):
    """
    Fetch all patients (optionally filtered by project_id) with a count of how many samples they have.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = """
            SELECT p.id, p.project_id, p.ext_patient_id, p.ext_patient_url,
                   p.public_patient_id, COUNT(s.id) AS sample_count
            FROM patients p
            LEFT JOIN samples s ON p.id = s.patient_id
        """
        params = []

        if project_id is not None:
            query += " WHERE p.project_id = %s"
            params.append(project_id)

        query += " GROUP BY p.id ORDER BY p.id"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        patients = []
        for row in rows:
            patients.append({
                'id': row[0],
                'project_id': row[1],
                'ext_patient_id': row[2],
                'ext_patient_url': row[3],
                'public_patient_id': row[4],
                'sample_count': row[5]
            })

        return patients

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/projects/", response_model=List[Project])
async def get_projects():
    """
    Fetch all projects from the database.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, status FROM projects")
        rows = cursor.fetchall()
        conn.close()

        return [Project(id=row[0], name=row[1], status=row[2]) for row in rows]

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/datasets/", response_model=List[Dataset])
async def get_datasets(
        project_id: Optional[int] = Query(None, description="Filter by project ID"),
        dataset_id: Optional[int] = Query(None, description="Filter by dataset ID")
):
    """
    Fetch datasets, optionally filtered by project_id and/or dataset_id.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = "SELECT id, project_id, name FROM datasets WHERE 1=1"
        params = []

        if project_id is not None:
            query += " AND project_id = %s"
            params.append(project_id)

        if dataset_id is not None:
            query += " AND id = %s"
            params.append(dataset_id)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        return [Dataset(id=row[0], project_id=row[1], name=row[2]) for row in rows]

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/datasets_with_metadata/{dataset_id}", response_model=DatasetWithMetadata)
async def get_dataset_with_metadata(dataset_id: int, project_id: int):
    """
    Fetch dataset details (and its metadata) for the given dataset_id + project_id.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Fetch dataset
        cursor.execute(
            """
            SELECT id, project_id, name
            FROM datasets
            WHERE id = %s AND project_id = %s
            """,
            (dataset_id, project_id)
        )
        dataset_row = cursor.fetchone()
        if not dataset_row:
            raise HTTPException(status_code=404, detail="Dataset not found")

        # Fetch dataset metadata
        cursor.execute(
            """
            SELECT id, dataset_id, key, value
            FROM datasets_metadata
            WHERE dataset_id = %s
            """,
            (dataset_id,)
        )
        metadata_rows = cursor.fetchall()
        conn.close()

        dataset = {
            "id": dataset_row[0],
            "project_id": dataset_row[1],
            "name": dataset_row[2],
            "metadata": [
                {"id": row[0], "dataset_id": row[1], "key": row[2], "value": row[3]}
                for row in metadata_rows
            ],
        }
        return dataset

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.get("/raw_files_with_metadata/{dataset_id}", response_model=List[RawFileResponse])
async def get_raw_files_with_metadata(dataset_id: int):
    """
    Fetch raw_files within a dataset, along with any related sample metadata.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Query raw files and the sample relationship from raw_files_metadata
        query = """
            SELECT rf.id, rf.path, rfm.metadata_value AS sample_id, s.ext_sample_id
            FROM raw_files rf
            LEFT JOIN raw_files_metadata rfm ON rf.id = rfm.raw_file_id
            LEFT JOIN samples s ON rfm.metadata_value = s.id
            WHERE rf.dataset_id = %s AND rfm.metadata_key = 'sample_id'
        """
        cursor.execute(query, (dataset_id,))
        raw_files = cursor.fetchall()

        response = []

        for (raw_file_id, path, sample_id, ext_sample_id) in raw_files:
            # Fetch sample metadata
            cursor.execute(
                """
                SELECT id, sample_id, key, value
                FROM samples_metadata
                WHERE sample_id = %s
                """,
                (sample_id,)
            )
            sample_metadata_rows = cursor.fetchall()
            sample_metadata_list = []
            for row in sample_metadata_rows:
                sample_metadata_list.append({
                    'id': row[0],
                    'sample_id': row[1],
                    'key': row[2],
                    'value': row[3]
                })

            response.append(RawFileResponse(
                id=raw_file_id,
                path=path,
                sample_id=sample_id,
                ext_sample_id=ext_sample_id,
                sample_metadata=sample_metadata_list
            ))

        conn.close()
        return response

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@router.put("/datasets_metadata/size_update", response_model=MetadataUpdate)
def update_metadata(update: MetadataUpdate):
    """
    Update specific metadata fields in the datasets_metadata table:
      - raw_file_extension_size_of_all_files
      - last_size_update
    for the given dataset_id.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 1) Update or insert 'raw_file_extension_size_of_all_files'
        if update.raw_file_size:
            cursor.execute(
                """
                SELECT id, value
                FROM datasets_metadata
                WHERE key = 'raw_file_extension_size_of_all_files'
                  AND dataset_id = %s
                """,
                (update.dataset_id,)
            )
            record = cursor.fetchone()
            if record:
                record_id, _ = record
                cursor.execute(
                    """
                    UPDATE datasets_metadata
                    SET value = %s
                    WHERE id = %s
                    """,
                    (update.raw_file_size, record_id)
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO datasets_metadata (dataset_id, key, value)
                    VALUES (%s, 'raw_file_extension_size_of_all_files', %s)
                    """,
                    (update.dataset_id, update.raw_file_size)
                )

        # 2) Update or insert 'last_size_update'
        if update.last_size_update:
            cursor.execute(
                """
                SELECT id, value
                FROM datasets_metadata
                WHERE key = 'last_size_update'
                  AND dataset_id = %s
                """,
                (update.dataset_id,)
            )
            record = cursor.fetchone()
            if record:
                record_id, _ = record
                cursor.execute(
                    """
                    UPDATE datasets_metadata
                    SET value = %s
                    WHERE id = %s
                    """,
                    (update.last_size_update, record_id)
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO datasets_metadata (dataset_id, key, value)
                    VALUES (%s, 'last_size_update', %s)
                    """,
                    (update.dataset_id, update.last_size_update)
                )

        conn.commit()
        conn.close()
        return update

    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")