from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import Integer, String, DateTime, func, Float, Date, func

class Base(DeclarativeBase):
    pass


from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import Integer, String, DateTime, Date, func

class Base(DeclarativeBase):
    pass

class AdmissionDischarge(Base):
    __tablename__ = "admissiondischarge"
    id = mapped_column(Integer, primary_key=True)
    batch_id = mapped_column(String(64), nullable=False)
    sender_id = mapped_column(String(100), nullable=False)
    report_date = mapped_column(Date, nullable=False)
    sent_at = mapped_column(DateTime, nullable=False)
    version = mapped_column(String(50), nullable=False)
    encounter_id = mapped_column(String(250), nullable=False)
    event = mapped_column(String(32), nullable=False)
    recorded_at = mapped_column(DateTime, nullable=False)
    patient_age = mapped_column(Integer, nullable=False)
    trace_id = mapped_column(String(64), nullable=False)
    date_created = mapped_column(DateTime, nullable=False, default=func.now())

    def to_dict(self):
        return {
            "id": self.id,
            "batch_id": self.batch_id,
            "sender_id": self.sender_id,
            "report_date": str(self.report_date),
            "sent_at": self.sent_at.isoformat(),
            "version": self.version,
            "encounter_id": self.encounter_id,
            "event": self.event,
            "recorded_at": self.recorded_at.isoformat(),
            "patient_age": self.patient_age,
            "trace_id": self.trace_id,
            "date_created": self.date_created.isoformat(),
        }


# class AdmissionDischarge(Base):
#     __tablename__ = "admissiondischarge"
#     id = mapped_column(Integer, primary_key=True)

#     batch_id = mapped_column(String(64), nullable=False)
#     sender_id = mapped_column(String(100), nullable=False)
#     report_date = mapped_column(Date, nullable=False)
#     sent_at = mapped_column(DateTime, nullable=False)
#     version = mapped_column(String(50), nullable=False)

#     encounter_id = mapped_column(String(250), nullable=False)
#     event = mapped_column(String(32), nullable=False)
#     recorded_at = mapped_column(DateTime, nullable=False)
#     patient_age = mapped_column(Integer, nullable=False)

#     trace_id = mapped_column(String(64), nullable=False)

#     date_created = mapped_column(DateTime, nullable=False, default=func.now())


# class Capacity(Base):
#     __tablename__ = "capacity"
#     id = mapped_column(Integer, primary_key=True)

#     batch_id = mapped_column(String(64), nullable=False)
#     sender_id = mapped_column(String(250), nullable=False)
#     report_date = mapped_column(Date, nullable=False)
#     sent_at = mapped_column(DateTime, nullable=False)
#     version = mapped_column(String(50), nullable=False)

#     unit_id = mapped_column(String(250), nullable=False)
#     total_beds = mapped_column(Integer, nullable=False)
#     occupied_beds = mapped_column(Integer, nullable=False)
#     recorded_at = mapped_column(DateTime, nullable=False)

#     trace_id = mapped_column(String(64), nullable=False)

#     date_created = mapped_column(DateTime, nullable=False, default=func.now())


class Capacity(Base):
    __tablename__ = "capacity"

    id = mapped_column(Integer, primary_key=True)

    batch_id = mapped_column(String(64), nullable=False)
    sender_id = mapped_column(String(250), nullable=False)
    report_date = mapped_column(Date, nullable=False)
    sent_at = mapped_column(DateTime, nullable=False)
    version = mapped_column(String(50), nullable=False)

    unit_id = mapped_column(String(250), nullable=False)
    total_beds = mapped_column(Integer, nullable=False)
    occupied_beds = mapped_column(Integer, nullable=False)
    recorded_at = mapped_column(DateTime, nullable=False)

    trace_id = mapped_column(String(64), nullable=False)
    date_created = mapped_column(DateTime, nullable=False, default=func.now())

    def to_dict(self):
        return {
            "id": self.id,
            "batch_id": self.batch_id,
            "sender_id": self.sender_id,
            "report_date": str(self.report_date),
            "sent_at": self.sent_at.isoformat(),
            "version": self.version,
            "unit_id": self.unit_id,
            "total_beds": self.total_beds,
            "occupied_beds": self.occupied_beds,
            "recorded_at": self.recorded_at.isoformat(),
            "trace_id": self.trace_id,
            "date_created": self.date_created.isoformat(),
        }
