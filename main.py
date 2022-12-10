from os import listdir, getenv
from os.path import isfile, join
import shutil 
import csv 
from dotenv import load_dotenv

from supabase import create_client, Client

load_dotenv()

INPUT_FILES_PATH = getenv("INPUT_FILES_PATH")
PROCESSED_FILES_PATH = getenv("PROCESSED_FILES_PATH")
DEBUG = getenv("DEBUG")

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
    time = f"{time[0][1:]}:{time[1]}:{time[2]}"
    return f"{year} {time}"

def process(f, data):

    date = createDate(f)

    for row in data:

        #if this is not pupil data, skill it.
        if row["student"] == "":
            continue

        className = row["section"]
        pupil = row["student"]
        for key in list(row.keys())[4:]:
            formativeName = key
            formativeScore = row[formativeName]

        if (formativeScore != ""):
            print(date, className, pupil, formativeName, formativeScore)




def clean(f):
    print(f"Moving {f}")

    if (DEBUG == False):
        shutil.move(join(INPUT_FILES_PATH,f), f"{PROCESSED_FILES_PATH}/{f}")
    
        



for f in getInputFiles():
    data = loadCSV(f)
    process(f, data)
    clean(f)


