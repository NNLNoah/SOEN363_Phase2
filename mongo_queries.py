"""
MongoDB equivalents of q1.sql .. q20.sql.

Collections:
  Admissions      _id=admission_id, patient_id, admission_time, discharge_time,
                  time_of_death, admission_source, admission_type, ...,
                  diagnoses[], clinical_notes[], radiology_exams[],
                  surgical_procedures[],
                  icu_ccu_stays[{stay_id, unit_type, entry_time, exit_time,
                                 transfers[{from_unit, to_unit, transfer_time}]}]
  Patients        _id=patient_id, first_name, last_name, admission_ids[],
                  physician_visits[]
  IcdDictionary   _id={icd_code, icd_version}, short_title, long_title

Every function returns a (collection_name, pipeline) pair so benchmark.py can run:
    list(db[coll].aggregate(pipeline))
"""

# Q1 — stays that start AND end in MICU (first/last transfer per stay)
def q1():
    return "Admissions", [
        {"$unwind": "$icu_ccu_stays"},
        {"$addFields": {
            "sorted_transfers": {
                "$sortArray": {"input": "$icu_ccu_stays.transfers",
                               "sortBy": {"transfer_time": 1}}
            }
        }},
        {"$match": {
            "$expr": {
                "$and": [
                    {"$gt": [{"$size": "$sorted_transfers"}, 0]},
                    {"$eq": [{"$first": "$sorted_transfers.from_unit"}, "MICU"]},
                    {"$eq": [{"$last":  "$sorted_transfers.to_unit"},   "MICU"]},
                ]
            }
        }},
        {"$lookup": {"from": "Patients", "localField": "patient_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "patient_id": 1, "admission_id": "$_id",
            "stay_id": "$icu_ccu_stays.stay_id",
            "unit_type": "$icu_ccu_stays.unit_type",
            "first_care_unit": {"$first": "$sorted_transfers.from_unit"},
            "last_care_unit":  {"$last":  "$sorted_transfers.to_unit"},
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
        }},
        {"$sort": {"patient_id": 1}},
    ]

# Q2 — patients with more than 3 admissions
def q2():
    return "Patients", [
        {"$project": {
            "patient_id": "$_id",
            "patient_name": {"$concat": ["$first_name", " ", "$last_name"]},
            "total_admissions": {"$size": "$admission_ids"},
        }},
        {"$match": {"total_admissions": {"$gt": 3}}},
        {"$sort": {"total_admissions": -1}},
    ]

# Q3 — discharged admissions with no surgical procedure
def q3():
    return "Admissions", [
        {"$match": {"discharge_time": {"$ne": None},
                    "surgical_procedures": {"$size": 0}}},
        {"$lookup": {"from": "Patients", "localField": "patient_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "admission_id": "$_id", "admission_time": 1, "discharge_time": 1,
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
        }},
        {"$sort": {"admission_time": 1}},
    ]

# Q4 — admissions with both a surgery AND a radiology exam
def q4():
    return "Admissions", [
        {"$match": {
            "surgical_procedures.0": {"$exists": True},
            "radiology_exams.0":     {"$exists": True},
        }},
        {"$lookup": {"from": "Patients", "localField": "patient_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "patient_id": 1, "admission_id": "$_id", "admission_time": 1,
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
        }},
        {"$sort": {"patient_id": 1}},
    ]

# Q5 — ICU/CCU stays longer than 7 days
def q5():
    seven_days_ms = 7 * 86400 * 1000
    return "Admissions", [
        {"$unwind": "$icu_ccu_stays"},
        {"$match": {
            "icu_ccu_stays.entry_time": {"$ne": None},
            "icu_ccu_stays.exit_time":  {"$ne": None},
            "$expr": {"$gt": [
                {"$subtract": ["$icu_ccu_stays.exit_time", "$icu_ccu_stays.entry_time"]},
                seven_days_ms]},
        }},
        {"$lookup": {"from": "Patients", "localField": "patient_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
            "stay_id":    "$icu_ccu_stays.stay_id",
            "unit_type":  "$icu_ccu_stays.unit_type",
            "entry_time": "$icu_ccu_stays.entry_time",
            "exit_time":  "$icu_ccu_stays.exit_time",
            "stay_days": {"$divide": [
                {"$subtract": ["$icu_ccu_stays.exit_time", "$icu_ccu_stays.entry_time"]},
                86400000]},
        }},
        {"$sort": {"stay_days": -1}},
    ]

# Q6 — admission count per patient
def q6():
    return "Patients", [
        {"$project": {
            "patient_id": "$_id",
            "patient_name": {"$concat": ["$first_name", " ", "$last_name"]},
            "admission_count": {"$size": "$admission_ids"},
        }},
        {"$sort": {"admission_count": -1}},
    ]

# Q7 — ER admissions that led to an ICU stay
def q7():
    return "Admissions", [
        {"$match": {"admission_source": "Emergency Room",
                    "icu_ccu_stays.0": {"$exists": True}}},
        {"$lookup": {"from": "Patients", "localField": "patient_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "patient_id": 1, "admission_id": "$_id", "admission_source": 1,
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
        }},
        {"$sort": {"patient_id": 1}},
    ]

# Q8 — ICD code frequency among ICU patients
def q8():
    return "Admissions", [
        {"$match": {"icu_ccu_stays.0": {"$exists": True}}},
        {"$unwind": "$diagnoses"},
        {"$group": {
            "_id": {"icd_code": "$diagnoses.icd_code",
                    "icd_version": "$diagnoses.icd_version"},
            "frequency": {"$sum": 1}}},
        {"$lookup": {"from": "IcdDictionary", "localField": "_id",
                     "foreignField": "_id", "as": "icd"}},
        {"$unwind": {"path": "$icd", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "icd_code": "$_id.icd_code", "icd_version": "$_id.icd_version",
            "short_title": "$icd.short_title", "frequency": 1, "_id": 0,
        }},
        {"$sort": {"frequency": -1}},
    ]

# Q9 — length-of-stay stats per ICU unit type
def q9():
    return "Admissions", [
        {"$unwind": "$icu_ccu_stays"},
        {"$match": {"icu_ccu_stays.entry_time": {"$ne": None},
                    "icu_ccu_stays.exit_time":  {"$ne": None}}},
        {"$project": {
            "unit_type": "$icu_ccu_stays.unit_type",
            "days": {"$divide": [
                {"$subtract": ["$icu_ccu_stays.exit_time",
                               "$icu_ccu_stays.entry_time"]},
                86400000]},
        }},
        {"$group": {
            "_id": "$unit_type",
            "total_stays": {"$sum": 1},
            "avg_days": {"$avg": "$days"},
            "min_days": {"$min": "$days"},
            "max_days": {"$max": "$days"},
        }},
        {"$sort": {"avg_days": -1}},
    ]

# Q10 — surgeries performed before ICU entry (same admission)
def q10():
    return "Admissions", [
        {"$match": {"surgical_procedures.0": {"$exists": True},
                    "icu_ccu_stays.0": {"$exists": True}}},
        {"$unwind": "$surgical_procedures"},
        {"$unwind": "$icu_ccu_stays"},
        {"$match": {"$expr": {"$lt": ["$surgical_procedures.procedure_datetime",
                                      "$icu_ccu_stays.entry_time"]}}},
        {"$lookup": {"from": "Patients", "localField": "patient_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "patient_id": 1, "admission_id": "$_id",
            "procedure_type":     "$surgical_procedures.procedure_type",
            "procedure_datetime": "$surgical_procedures.procedure_datetime",
            "unit_type":          "$icu_ccu_stays.unit_type",
            "icu_entry_time":     "$icu_ccu_stays.entry_time",
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
        }},
        {"$sort": {"patient_id": 1}},
    ]

# Q11 — total radiology exams per patient
def q11():
    return "Admissions", [
        {"$group": {"_id": "$patient_id",
                    "total_radiology_exams": {"$sum": {"$size": "$radiology_exams"}}}},
        {"$match": {"total_radiology_exams": {"$gt": 0}}},
        {"$lookup": {"from": "Patients", "localField": "_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "patient_id": "$_id", "total_radiology_exams": 1, "_id": 0,
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
        }},
        {"$sort": {"total_radiology_exams": -1}},
    ]

# Q12 — clinical notes containing 'recovery'
def q12():
    return "Admissions", [
        {"$match": {"clinical_notes.note_text": {"$regex": "recovery", "$options": "i"}}},
        {"$unwind": "$clinical_notes"},
        {"$match": {"clinical_notes.note_text": {"$regex": "recovery", "$options": "i"}}},
        {"$lookup": {"from": "Patients", "localField": "patient_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "patient_id": 1,
            "note_type":     "$clinical_notes.note_type",
            "note_datetime": "$clinical_notes.note_datetime",
            "note_text":     "$clinical_notes.note_text",
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
        }},
        {"$sort": {"patient_id": 1}},
    ]

# Q13 — non-ICU admissions ranked by radiology count
def q13():
    return "Admissions", [
        {"$match": {"icu_ccu_stays": {"$size": 0},
                    "radiology_exams.0": {"$exists": True}}},
        {"$project": {
            "admission_id": "$_id", "patient_id": 1, "admission_time": 1,
            "radiology_count": {"$size": "$radiology_exams"}}},
        {"$lookup": {"from": "Patients", "localField": "patient_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "admission_id": 1, "admission_time": 1, "radiology_count": 1,
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
        }},
        {"$sort": {"radiology_count": -1}},
    ]

# Q14 — admission length-of-stay in days
def q14():
    return "Admissions", [
        {"$match": {"discharge_time": {"$ne": None}}},
        {"$lookup": {"from": "Patients", "localField": "patient_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "admission_id": "$_id", "admission_time": 1, "discharge_time": 1,
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
            "stay_days": {"$divide": [
                {"$subtract": ["$discharge_time", "$admission_time"]}, 86400000]},
        }},
        {"$sort": {"stay_days": -1}},
    ]

# Q15 — total ICU transfers per patient
def q15():
    return "Admissions", [
        {"$unwind": {"path": "$icu_ccu_stays", "preserveNullAndEmptyArrays": False}},
        {"$group": {
            "_id": "$patient_id",
            "total_transfers": {"$sum": {"$size": {"$ifNull": ["$icu_ccu_stays.transfers", []]}}}}},
        {"$match": {"total_transfers": {"$gt": 0}}},
        {"$lookup": {"from": "Patients", "localField": "_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "patient_id": "$_id", "total_transfers": 1, "_id": 0,
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
        }},
        {"$sort": {"total_transfers": -1}},
    ]

# Q16 — admissions that visited more than one distinct ICU unit type
def q16():
    return "Admissions", [
        {"$match": {"icu_ccu_stays.0": {"$exists": True}}},
        {"$project": {
            "admission_id": "$_id", "patient_id": 1,
            "icu_types": {"$setUnion": ["$icu_ccu_stays.unit_type", []]},
        }},
        {"$match": {"$expr": {"$gt": [{"$size": "$icu_types"}, 1]}}},
        {"$addFields": {"distinct_icu_types": {"$size": "$icu_types"}}},
        {"$lookup": {"from": "Patients", "localField": "patient_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "admission_id": 1, "distinct_icu_types": 1, "icu_types": 1,
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
        }},
        {"$sort": {"distinct_icu_types": -1}},
    ]

# Q17 — admissions with more than one diagnosis
def q17():
    return "Admissions", [
        {"$project": {
            "admission_id": "$_id", "patient_id": 1,
            "diagnosis_count": {"$size": "$diagnoses"}}},
        {"$match": {"diagnosis_count": {"$gt": 1}}},
        {"$lookup": {"from": "Patients", "localField": "patient_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "admission_id": 1, "diagnosis_count": 1,
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
        }},
        {"$sort": {"diagnosis_count": -1}},
    ]

# Q18 — most recent clinical note per patient
def q18():
    return "Admissions", [
        {"$unwind": "$clinical_notes"},
        {"$sort": {"patient_id": 1, "clinical_notes.note_datetime": -1}},
        {"$group": {
            "_id": "$patient_id",
            "note_id":       {"$first": "$clinical_notes.note_id"},
            "note_type":     {"$first": "$clinical_notes.note_type"},
            "note_datetime": {"$first": "$clinical_notes.note_datetime"},
            "note_text":     {"$first": "$clinical_notes.note_text"},
        }},
        {"$lookup": {"from": "Patients", "localField": "_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "patient_id": "$_id", "_id": 0,
            "note_id": 1, "note_type": 1, "note_datetime": 1, "note_text": 1,
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
        }},
        {"$sort": {"patient_id": 1}},
    ]

# Q19 — in-hospital deaths within the admission window
def q19():
    return "Admissions", [
        {"$match": {"time_of_death": {"$ne": None}}},
        {"$match": {"$expr": {"$and": [
            {"$gte": ["$time_of_death", "$admission_time"]},
            {"$lte": ["$time_of_death",
                      {"$ifNull": ["$discharge_time", "$time_of_death"]}]},
        ]}}},
        {"$lookup": {"from": "Patients", "localField": "patient_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "admission_id": "$_id", "admission_time": 1, "discharge_time": 1,
            "time_of_death": 1, "admission_type": 1,
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
        }},
        {"$sort": {"time_of_death": 1}},
    ]

# Q20 — surgery and radiology on the same day
def q20():
    return "Admissions", [
        {"$match": {"surgical_procedures.0": {"$exists": True},
                    "radiology_exams.0":     {"$exists": True}}},
        {"$unwind": "$surgical_procedures"},
        {"$unwind": "$radiology_exams"},
        {"$match": {"$expr": {"$eq": [
            {"$dateTrunc": {"date": "$surgical_procedures.procedure_datetime",
                            "unit": "day"}},
            {"$dateTrunc": {"date": "$radiology_exams.exam_datetime",
                            "unit": "day"}},
        ]}}},
        {"$lookup": {"from": "Patients", "localField": "patient_id",
                     "foreignField": "_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {
            "admission_id": "$_id",
            "shared_date": {"$dateTrunc": {"date": "$surgical_procedures.procedure_datetime",
                                           "unit": "day"}},
            "procedure_type": "$surgical_procedures.procedure_type",
            "exam_type":      "$radiology_exams.exam_type",
            "patient_name": {"$concat": ["$p.first_name", " ", "$p.last_name"]},
        }},
        {"$sort": {"shared_date": 1}},
    ]


ALL_QUERIES = [q1, q2, q3, q4, q5, q6, q7, q8, q9, q10,
               q11, q12, q13, q14, q15, q16, q17, q18, q19, q20]


if __name__ == "__main__":
    # Quick smoke test: print each query's target collection + stage count
    for i, fn in enumerate(ALL_QUERIES, 1):
        coll, pipe = fn()
        print(f"q{i:<2}  coll={coll:<14}  stages={len(pipe)}")
