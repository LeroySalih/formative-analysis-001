from os import listdir, getenv
from os.path import isfile, join
import shutil 
import csv 
from dotenv import load_dotenv

from supabase import create_client, Client
import datetime 

load_dotenv()

INPUT_FILES_PATH = getenv("INPUT_FILES_PATH")
PROCESSED_FILES_PATH = getenv("PROCESSED_FILES_PATH")
DEBUG = getenv("DEBUG") == "True"

print(INPUT_FILES_PATH, PROCESSED_FILES_PATH, DEBUG)

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

def process(f, data, client):

    date = createDate(f)

    for row in data:

        #if this is not pupil data, skill it.
        if row["student"] == "" :
            continue

        className = row["section"]
        pupil = row["student"]
        for key in list(row.keys())[4:]:
            formativeTitle = key
            formativeScore = row[formativeTitle]

            if (formativeScore != "" and formativeTitle != ""):
                formativeScore = int(formativeScore[:-1]) # remove the %
                
                updateObj= {"formativeTitle": formativeTitle, "className": className, "pupilName" : pupil, "score" : formativeScore, "uploadDate": date}
                client.table("gf_Submissions").upsert(updateObj).execute()
                print("Added", date, className, pupil, formativeTitle)
            


def clean(f):
    print(f"Moving {f}")

    if (DEBUG == False):
        shutil.move(join(INPUT_FILES_PATH,f), f"{PROCESSED_FILES_PATH}/{f}")
    
        

def getSupabaseClient ():
    url: str = getenv("SUPABASE_URL")
    key: str = getenv("SUPABASE_KEY")
    return create_client(url, key)



for f in getInputFiles():
    data = loadCSV(f)
    process(f, data, getSupabaseClient())
    clean(f)


