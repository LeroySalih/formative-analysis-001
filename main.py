from os import listdir, getenv
from os.path import isfile, join
import shutil 
import csv 
import copy
from dotenv import load_dotenv

from supabase import create_client, Client
import datetime

from datetime import datetime

load_dotenv()

INPUT_FILES_PATH = getenv("INPUT_FILES_PATH")
PROCESSED_FILES_PATH = getenv("PROCESSED_FILES_PATH")
DEBUG = getenv("DEBUG") == "True"
MONGO_URL = getenv("MONGO_URL") 

print(INPUT_FILES_PATH, PROCESSED_FILES_PATH, DEBUG)


def get_database():
 
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = MONGO_URL
 
   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
   client = MongoClient(CONNECTION_STRING)
 
   # Create the database for our example (we will use the same database throughout the tutorial
   return client['gf_analysis']


def getInputFiles ():

    files = [f for f in listdir(INPUT_FILES_PATH) if isfile (join(INPUT_FILES_PATH, f))]

    return files 


def loadCSV(f):
    print("Loading", f)
    data = []
    with open (join(INPUT_FILES_PATH, f)) as csvfile:
        reader = csv.DictReader(csvfile)

        for pupil in reader:
            data.append(pupil)

        return data

def createDate(f):

    date = f.split("-")[-1].split(".")[0]
    yearlst = date.split(",")[0].split("_")
    year = f"{yearlst[2]}-{yearlst[1]}-{yearlst[0][1:]}"
    time = date.split(",")[1]
    time = time.split("_")
    time = f"{time[0][1:]}:{time[2]}:{time[1]}"
    dt = f"{year} {time}"
    return dt


def get_latest_update():
    try:
        db = get_database()
        collection = db["uploads"]

        latest = list(collection.find().sort("date", -1))

        return latest[0]
    except:
        return None


def get_latest_update():
    try:
        db = get_database()
        collection = db["uploads"]

        latest = list(collection.find().sort("date", -1))

        return latest[0]
    except:
        return None


def write_to_supabase(client, updateObj):
    

    #Look for an existing 
   # result = client.table("gf_Submissions").select("*") \
   #     .eq("formativeTitle", updateObj["formativeTitle"]) \
   #     .eq("pupilName", updateObj["pupilName"] ) \
   #     .eq("className", updateObj["className"]) \
   #     .eq("score", updateObj["score"]) \
   #     .execute()


    #look to see if there is a record for this pupil, formtative, class and score
    result = client.table("gf_submissions.current").select("*") \
                        .eq("formativeTitle", updateObj["formativeTitle"]) \
                        .eq("pupilName", updateObj["pupilName"]) \
                        .eq("className", updateObj["className"]) \
                        .eq("score", updateObj["score"]) \
                        .execute()

    #if not, upsert one
    if (len(result.data) ==  0):
        print("Updating", updateObj["pupilName"], updateObj["uploadDate"], updateObj["score"])
        result = client.table("gf_submissions.current").upsert(updateObj).execute()
    else:
        print("Skipped", updateObj["pupilName"], updateObj["uploadDate"], updateObj["score"])
    





def process(f, data, client):

    date = createDate(f)

    for row in data:

        #if this is not pupil data, skip it.
        if row["student"] == "" :
            continue

        className = row["section"]
        pupil = row["student"]

        #loop through all the formatives
        for key in list(row.keys())[4:]:

            formativeTitle = key
            formativeScore = row[formativeTitle]

            if (formativeScore != "" and formativeTitle != ""):
                formativeScore = int(formativeScore[:-1]) # remove the %
                
                updateObj= {
                    "formativeTitle": formativeTitle, 
                    "className": className, 
                    "pupilName" : pupil, 
                    "score" : formativeScore, 
                    "uploadDate": date
                }
                
                
                #client.table("gf_Submissions").upsert(updateObj).execute()
                #print("Adding", date, className, pupil, formativeTitle, formativeScore)
                write_to_supabase(client, updateObj)

                


def clean(f):
    print(f"Moving {f}")

    if (DEBUG == False):
        shutil.move(join(INPUT_FILES_PATH,f), f"{PROCESSED_FILES_PATH}/{f}")
    
        

def getSupabaseClient ():
    url: str = getenv("SUPABASE_URL")
    key: str = getenv("SUPABASE_KEY")
    return create_client(url, key)


def fileNameToDate (fName):
    parts = fName.split(", ")
    dt = parts[0][-10:]
    time = parts[1][:8]
    

    date = int(dt[0:2])
    month = int(dt[3: 5])
    year = int(dt[6:10])

    hr = int(time[0:2])
    min = int(time[3:5])
    sec = int(time[6:8])

    return datetime(year, month, date, hr, min, sec)


#last_update = get_latest_update()
#print(last_update)

files = getInputFiles()
files = sorted(files, key=fileNameToDate)
for f in files:
    
    data = loadCSV(f)
    process(f, data, getSupabaseClient())
    clean(f)
