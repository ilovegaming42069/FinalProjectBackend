import sqlite3
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

DATABASE = "real_moringa.db"  # Use .db extension for SQLite database

def create_new_database():
    conn = sqlite3.connect(DATABASE)
    conn.execute('''
    CREATE TABLE IF NOT EXISTS batch_id (
        Batch_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        RawWeight INTEGER NOT NULL,
        InTimeRaw DATETIME NOT NULL,
        InTimeWet DATETIME,
        OutTimeWet DATETIME,
        WetWeight INTEGER,
        InTimeDry DATETIME,
        OutTimeDry DATETIME,
        DryWeight INTEGER,
        InTimePowder DATETIME,
        OutTimePowder DATETIME,
        PowderWeight INTEGER,
        Status TEXT NOT NULL,
        Centra_ID INTEGER NOT NULL,
        Package_ID INTEGER,
        WeightRescale INTEGER,
        FOREIGN KEY (Centra_ID) REFERENCES centra(Centra_ID),
        FOREIGN KEY (Package_ID) REFERENCES delivery(Package_ID)
    );
    ''')
    
    conn.execute('''
    CREATE TABLE IF NOT EXISTS centra (
        Centra_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        CentraName TEXT NOT NULL,
        CentraAddress TEXT NOT NULL,
        NumberOfEmployees INTEGER NOT NULL
    );
    ''')

    conn.execute('''
    CREATE TABLE IF NOT EXISTS delivery (
        Package_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Status TEXT NOT NULL,
        InDeliveryTime DATETIME NOT NULL,
        OutDeliveryTime DATETIME,
        ExpeditionType TEXT NOT NULL
    );
    ''')
    
    conn.commit()
    conn.close()
    print("New database created.")

# Create the database if it doesn't exist
if not os.path.exists(DATABASE):
    create_new_database()

app = FastAPI()

def get_db_connection():
    try:
        conn = sqlite3.connect(DATABASE)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.OperationalError as e:
        raise HTTPException(status_code=500, detail=str(e))

# Pydantic models
class Batch(BaseModel):
    Batch_ID: int
    RawWeight: int
    InTimeRaw: datetime
    InTimeWet: Optional[datetime] = None
    OutTimeWet: Optional[datetime] = None
    WetWeight: Optional[int] = None
    InTimeDry: Optional[datetime] = None
    OutTimeDry: Optional[datetime] = None
    DryWeight: Optional[int] = None
    InTimePowder: Optional[datetime] = None
    OutTimePowder: Optional[datetime] = None
    PowderWeight: Optional[int] = None
    Status: str
    Centra_ID: int
    Package_ID: Optional[int] = None
    WeightRescale: Optional[int] = None

class RawWeight(BaseModel):
    RawWeight: int
    InTimeRaw: datetime

class WetWeight(BaseModel):
    WetWeight: int
    InTimeWet: datetime
    OutTimeWet: datetime

class DryWeight(BaseModel):
    DryWeight: int
    InTimeDry: datetime
    OutTimeDry: datetime

class PowderWeight(BaseModel):
    PowderWeight: int
    InTimePowder: datetime
    OutTimePowder: datetime

class DeliveryInfo(BaseModel):
    Package_ID: int
    Status: str
    InDeliveryTime: datetime
    ExpeditionType: str

class UpdateStatus(BaseModel):
    Status: str

class OutDelivery(BaseModel):
    OutDeliveryTime: datetime
    WeightRescale: int

class BatchUpdateStatus(BaseModel):
    Batch_IDs: List[int]
    Status: str

class AddWetWeight(BaseModel):
    InTimeWet: datetime
    OutTimeWet: datetime
    WetWeight: int

class AddDryWeight(BaseModel):
    InTimeDry: datetime
    OutTimeDry: datetime
    DryWeight: int

class AddPowderWeight(BaseModel):
    InTimePowder: datetime
    OutTimePowder: datetime
    PowderWeight: int

class WeightInfo(BaseModel):
    Batch_ID: int
    RawWeight: int
    InTimeRaw: datetime

class NewBatch(BaseModel):
    RawWeight: int
    Status: str
    Centra_ID: int
    InTimeRaw: datetime

class NewPackage(BaseModel):
    InDeliveryTime: datetime
    ExpeditionType: str
    Status: str

class UpdatePackageID(BaseModel):
    Package_ID: int

class Centra(BaseModel):
    Centra_ID: int
    CentraName: str
    CentraAddress: str
    NumberOfEmployees: int

class NewCentra(BaseModel):
    CentraName: str
    CentraAddress: str
    NumberOfEmployees: int

class UpdateCentra(BaseModel):
    CentraName: Optional[str] = None
    CentraAddress: Optional[str] = None
    NumberOfEmployees: Optional[int] = None

# FastAPI endpoints
@app.get("/batches", response_model=List[Batch])
def get_all_batches():
    conn = get_db_connection()
    batches = conn.execute('SELECT * FROM batch_id').fetchall()
    conn.close()
    return [
        Batch(
            Batch_ID=batch['Batch_ID'],
            RawWeight=batch['RawWeight'],
            InTimeRaw=batch['InTimeRaw'],
            InTimeWet=batch['InTimeWet'],
            OutTimeWet=batch['OutTimeWet'],
            WetWeight=batch['WetWeight'],
            InTimeDry=batch['InTimeDry'],
            OutTimeDry=batch['OutTimeDry'],
            DryWeight=batch['DryWeight'],
            InTimePowder=batch['InTimePowder'],
            OutTimePowder=batch['OutTimePowder'],
            PowderWeight=batch['PowderWeight'],
            Status=batch['Status'],
            Centra_ID=batch['Centra_ID'],
            Package_ID=batch['Package_ID'],
            WeightRescale=batch['WeightRescale']
        )
        for batch in batches
    ]

@app.get("/batch/{batch_id}/raw", response_model=RawWeight)
def get_raw_weight(batch_id: int):
    conn = get_db_connection()
    raw = conn.execute('SELECT RawWeight, InTimeRaw FROM batch_id WHERE Batch_ID = ?', (batch_id,)).fetchone()
    conn.close()
    if raw is None:
        raise HTTPException(status_code=404, detail="Batch not found")
    return RawWeight(RawWeight=raw['RawWeight'], InTimeRaw=raw['InTimeRaw'])

@app.get("/batch/{batch_id}/wet", response_model=WetWeight)
def get_wet_weight(batch_id: int):
    conn = get_db_connection()
    wet = conn.execute('SELECT WetWeight, InTimeWet, OutTimeWet FROM batch_id WHERE Batch_ID = ?', (batch_id,)).fetchone()
    conn.close()
    if wet is None:
        raise HTTPException(status_code=404, detail="Batch not found")
    return WetWeight(WetWeight=wet['WetWeight'], InTimeWet=wet['InTimeWet'], OutTimeWet=wet['OutTimeWet'])

@app.get("/batch/{batch_id}/dry", response_model=DryWeight)
def get_dry_weight(batch_id: int):
    conn = get_db_connection()
    dry = conn.execute('SELECT DryWeight, InTimeDry, OutTimeDry FROM batch_id WHERE Batch_ID = ?', (batch_id,)).fetchone()
    conn.close()
    if dry is None:
        raise HTTPException(status_code=404, detail="Batch not found")
    return DryWeight(DryWeight=dry['DryWeight'], InTimeDry=dry['InTimeDry'], OutTimeDry=dry['OutTimeDry'])

@app.get("/batch/{batch_id}/powder", response_model=PowderWeight)
def get_powder_weight(batch_id: int):
    conn = get_db_connection()
    powder = conn.execute('SELECT PowderWeight, InTimePowder, OutTimePowder FROM batch_id WHERE Batch_ID = ?', (batch_id,)).fetchone()
    conn.close()
    if powder is None:
        raise HTTPException(status_code=404, detail="Batch not found")
    return PowderWeight(PowderWeight=powder['PowderWeight'], InTimePowder=powder['InTimePowder'], OutTimePowder=powder['OutTimePowder'])

@app.get("/batch/{batch_id}/delivery", response_model=DeliveryInfo)
def get_delivery_info(batch_id: int):
    conn = get_db_connection()
    delivery = conn.execute('''
        SELECT d.Package_ID, d.Status, d.InDeliveryTime, d.ExpeditionType
        FROM delivery d
        JOIN batch_id b ON b.Package_ID = d.Package_ID
        WHERE b.Batch_ID = ?
    ''', (batch_id,)).fetchone()
    conn.close()
    if delivery is None:
        raise HTTPException(status_code=404, detail="Batch not found")
    return DeliveryInfo(Package_ID=delivery['Package_ID'], Status=delivery['Status'], InDeliveryTime=delivery['InDeliveryTime'], ExpeditionType=delivery['ExpeditionType'])

@app.get("/packages", response_model=List[DeliveryInfo])
def get_all_packages():
    conn = get_db_connection()
    packages = conn.execute('SELECT Package_ID, Status, InDeliveryTime, ExpeditionType FROM delivery').fetchall()
    conn.close()
    return [DeliveryInfo(Package_ID=package['Package_ID'], Status=package['Status'], InDeliveryTime=package['InDeliveryTime'], ExpeditionType=package['ExpeditionType']) for package in packages]

@app.put("/package/{package_id}/status", response_model=UpdateStatus)
def update_package_status(package_id: int, status: UpdateStatus):
    conn = get_db_connection()
    conn.execute('UPDATE delivery SET Status = ? WHERE Package_ID = ?', (status.Status, package_id))
    conn.commit()
    conn.close()
    return status

@app.put("/batch/{batch_id}/status", response_model=UpdateStatus)
def update_delivery_status(batch_id: int, status: UpdateStatus):
    conn = get_db_connection()
    package_id = conn.execute('SELECT Package_ID FROM batch_id WHERE Batch_ID = ?', (batch_id,)).fetchone()
    if package_id is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Batch not found")
    package_id = package_id['Package_ID']
    conn.execute('UPDATE delivery SET Status = ? WHERE Package_ID = ?', (status.Status, package_id))
    conn.commit()
    conn.close()
    return status

@app.put("/batch/{batch_id}/outdelivery", response_model=OutDelivery)
def update_outdelivery(batch_id: int, outdelivery: OutDelivery):
    conn = get_db_connection()
    conn.execute('UPDATE delivery SET OutDeliveryTime = ?, WeightRescale = ? WHERE Package_ID = (SELECT Package_ID FROM batch_id WHERE Batch_ID = ?)', 
                 (outdelivery.OutDeliveryTime, outdelivery.WeightRescale, batch_id))
    conn.commit()
    conn.close()
    return outdelivery

@app.put("/batches/status", response_model=BatchUpdateStatus)
def update_batches_status(batch_update: BatchUpdateStatus):
    conn = get_db_connection()
    for batch_id in batch_update.Batch_IDs:
        conn.execute('UPDATE batch_id SET Status = ? WHERE Batch_ID = ?', (batch_update.Status, batch_id))
    conn.commit()
    conn.close()
    return batch_update

@app.get("/centra/{centra_id}/batches/weights", response_model=List[WeightInfo])
def get_last_seven_batches_weights(centra_id: int):
    conn = get_db_connection()
    weights = conn.execute('''
        SELECT Batch_ID, RawWeight, InTimeRaw
        FROM batch_id
        WHERE Centra_ID = ?
        ORDER BY InTimeRaw DESC
        LIMIT 7
    ''', (centra_id,)).fetchall()
    conn.close()
    return [WeightInfo(Batch_ID=weight['Batch_ID'], RawWeight=weight['RawWeight'], InTimeRaw=weight['InTimeRaw']) for weight in weights]

@app.get("/centra/{centra_id}/batches", response_model=List[Batch])
def get_all_batches_from_centra(centra_id: int):
    conn = get_db_connection()
    batches = conn.execute('SELECT Batch_ID FROM batch_id WHERE Centra_ID = ?', (centra_id,)).fetchall()
    conn.close()
    return [Batch(Batch_ID=batch['Batch_ID']) for batch in batches]

@app.put("/batch/{batch_id}/wet", response_model=WetWeight)
def add_wet_weight(batch_id: int, wet: AddWetWeight):
    conn = get_db_connection()
    conn.execute('''
        UPDATE batch_id
        SET InTimeWet = ?, OutTimeWet = ?, WetWeight = ?
        WHERE Batch_ID = ?
    ''', (wet.InTimeWet, wet.OutTimeWet, wet.WetWeight, batch_id))
    conn.commit()
    conn.close()
    return WetWeight(WetWeight=wet.WetWeight, InTimeWet=wet.InTimeWet, OutTimeWet=wet.OutTimeWet)

@app.put("/batch/{batch_id}/dry", response_model=DryWeight)
def add_dry_weight(batch_id: int, dry: AddDryWeight):
    conn = get_db_connection()
    conn.execute('''
        UPDATE batch_id
        SET InTimeDry = ?, OutTimeDry = ?, DryWeight = ?
        WHERE Batch_ID = ?
    ''', (dry.InTimeDry, dry.OutTimeDry, dry.DryWeight, batch_id))
    conn.commit()
    conn.close()
    return DryWeight(DryWeight=dry.DryWeight, InTimeDry=dry.InTimeDry, OutTimeDry=dry.OutTimeDry)

@app.put("/batch/{batch_id}/powder", response_model=PowderWeight)
def add_powder_weight(batch_id: int, powder: AddPowderWeight):
    conn = get_db_connection()
    conn.execute('''
        UPDATE batch_id
        SET InTimePowder = ?, OutTimePowder = ?, PowderWeight = ?
        WHERE Batch_ID = ?
    ''', (powder.InTimePowder, powder.OutTimePowder, powder.PowderWeight, batch_id))
    conn.commit()
    conn.close()
    return PowderWeight(PowderWeight=powder.PowderWeight, InTimePowder=powder.InTimePowder, OutTimePowder=powder.OutTimePowder)

@app.post("/batch", response_model=Batch)
def add_batch(batch: NewBatch):
    conn = get_db_connection()
    cursor = conn.execute('''
        INSERT INTO batch_id (RawWeight, Status, Centra_ID, InTimeRaw)
        VALUES (?, ?, ?, ?)
    ''', (batch.RawWeight, batch.Status, batch.Centra_ID, batch.InTimeRaw))
    batch_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return Batch(Batch_ID=batch_id)

@app.post("/package", response_model=DeliveryInfo)
def add_package(package: NewPackage):
    conn = get_db_connection()
    cursor = conn.execute('''
        INSERT INTO delivery (InDeliveryTime, ExpeditionType, Status)
        VALUES (?, ?, ?)
    ''', (package.InDeliveryTime, package.ExpeditionType, package.Status))
    package_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return DeliveryInfo(Package_ID=package_id, Status=package.Status, InDeliveryTime=package.InDeliveryTime, ExpeditionType=package.ExpeditionType)

@app.put("/batch/{batch_id}/package", response_model=Batch)
def update_package_id(batch_id: int, package: UpdatePackageID):
    conn = get_db_connection()
    conn.execute('''
        UPDATE batch_id
        SET Package_ID = ?
        WHERE Batch_ID = ?
    ''', (package.Package_ID, batch_id))
    conn.commit()
    conn.close()
    return Batch(Batch_ID=batch_id)

@app.get("/centra", response_model=List[Centra])
def get_all_centra():
    conn = get_db_connection()
    centra = conn.execute('SELECT Centra_ID, CentraName, CentraAddress, NumberOfEmployees FROM centra').fetchall()
    conn.close()
    return [Centra(Centra_ID=c['Centra_ID'], CentraName=c['CentraName'], CentraAddress=c['CentraAddress'], NumberOfEmployees=c['NumberOfEmployees']) for c in centra]

@app.put("/centra/{centra_id}", response_model=Centra)
def update_centra(centra_id: int, centra: UpdateCentra):
    conn = get_db_connection()
    existing_centra = conn.execute('SELECT * FROM centra WHERE Centra_ID = ?', (centra_id,)).fetchone()
    if existing_centra is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Centra not found")

    update_data = centra.dict(exclude_unset=True)
    for key, value in update_data.items():
        conn.execute(f'UPDATE centra SET {key} = ? WHERE Centra_ID = ?', (value, centra_id))
    
    conn.commit()
    updated_centra = conn.execute('SELECT * FROM centra WHERE Centra_ID = ?', (centra_id,)).fetchone()
    conn.close()
    return Centra(Centra_ID=updated_centra['Centra_ID'], CentraName=updated_centra['CentraName'], CentraAddress=updated_centra['CentraAddress'], NumberOfEmployees=updated_centra['NumberOfEmployees'])

@app.delete("/centra/{centra_id}", response_model=Centra)
def delete_centra(centra_id: int):
    conn = get_db_connection()
    centra = conn.execute('SELECT * FROM centra WHERE Centra_ID = ?', (centra_id,)).fetchone()
    if centra is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Centra not found")
    conn.execute('DELETE FROM centra WHERE Centra_ID = ?', (centra_id,))
    conn.commit()
    conn.close()
    return Centra(Centra_ID=centra['Centra_ID'], CentraName=centra['CentraName'], CentraAddress=centra['CentraAddress'], NumberOfEmployees=centra['NumberOfEmployees'])

@app.delete("/package/{package_id}", response_model=DeliveryInfo)
def delete_package(package_id: int):
    conn = get_db_connection()
    package = conn.execute('SELECT * FROM delivery WHERE Package_ID = ?', (package_id,)).fetchone()
    if package is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Package not found")
    conn.execute('DELETE FROM delivery WHERE Package_ID = ?', (package_id,))
    conn.commit()
    conn.close()
    return DeliveryInfo(Package_ID=package['Package_ID'], Status=package['Status'], InDeliveryTime=package['InDeliveryTime'], ExpeditionType=package['ExpeditionType'])

@app.delete("/batch/{batch_id}", response_model=Batch)
def delete_batch(batch_id: int):
    conn = get_db_connection()
    batch = conn.execute('SELECT * FROM batch_id WHERE Batch_ID = ?', (batch_id,)).fetchone()
    if batch is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Batch not found")
    conn.execute('DELETE FROM batch_id WHERE Batch_ID = ?', (batch_id,))
    conn.commit()
    conn.close()
    return Batch(
        Batch_ID=batch['Batch_ID'],
        RawWeight=batch['RawWeight'],
        InTimeRaw=batch['InTimeRaw'],
        InTimeWet=batch['InTimeWet'],
        OutTimeWet=batch['OutTimeWet'],
        WetWeight=batch['WetWeight'],
        InTimeDry=batch['InTimeDry'],
        OutTimeDry=batch['OutTimeDry'],
        DryWeight=batch['DryWeight'],
        InTimePowder=batch['InTimePowder'],
        OutTimePowder=batch['OutTimePowder'],
        PowderWeight=batch['PowderWeight'],
        Status=batch['Status'],
        Centra_ID=batch['Centra_ID'],
        Package_ID=batch['Package_ID'],
        WeightRescale=batch['WeightRescale']
    )

@app.post("/centra", response_model=Centra)
def add_centra(centra: NewCentra):
    conn = get_db_connection()
    cursor = conn.execute('''
        INSERT INTO centra (CentraName, CentraAddress, NumberOfEmployees)
        VALUES (?, ?, ?)
    ''', (centra.CentraName, centra.CentraAddress, centra.NumberOfEmployees))
    centra_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return Centra(
        Centra_ID=centra_id,
        CentraName=centra.CentraName,
        CentraAddress=centra.CentraAddress,
        NumberOfEmployees=centra.NumberOfEmployees
    )
