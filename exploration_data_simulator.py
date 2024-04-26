# Import Libraries to generate data set
import json
import random
import shutil
import os
import pandas as pd
from faker import Faker


# Create faker object
fake = Faker()
def drilling_logs_simulator(sites, wells, records):
    """
    This function takes in three arguments to generate drilling log data.
    :param sites: Number of sites
    :param wells: Number of wells per site
    :param records: Number of records you want to generate per well
    :return: Dictionary of dataframes, where each dataframe contains drilling log data for a site
    """
    # Create empty dictionary to store dataframes
    drilling_logs = {}

    # Create directory to output each file
    cwd = os.getcwd()
    path_name = "drilling_logs"
    new_path = os.path.join(cwd, path_name)
    if os.path.exists(new_path):
        shutil.rmtree(new_path)
    os.makedirs(new_path, exist_ok=True)
    for site_index in range(1, sites + 1):
        sid = f"S{site_index:03}"
        # Create empty list to store rows of data
        site_logs = []
        for well_index in range(1, wells+1):
            wid = f"{sid}-W{well_index:03}"
            for record in range(1, records+1):
                # Generate drilling logs data variables
                well_id = wid  # Unique identifier for each well
                depth = random.randint(100, 5000)  # Depth at which the drilling parameters were recorded (in meters)
                lithology = random.choice(["Sandstone", "Shale", "Limestone", "Granite"])  # Rock formation drilled
                formation_thickness = random.randint(1, 90)  # Thickness of the drilled formation (in meters)
                rop = random.uniform(1, 30)  # Rate of Penetration (in feet per hour)
                wob = random.uniform(1000, 5000)  # Weight on Bit (in pounds)
                rotary_speed = random.uniform(50, 200)  # Rotary speed (in RPM)
                torque = random.uniform(5000, 20000)  # Torque (in foot-pounds)
                mud_flow_rate = random.uniform(50, 500)  # Mud flow rate (in gallons per minute)
                mud_weight = random.uniform(9, 15)  # Mud weight (in pounds per gallon)
                hookload = random.uniform(10000, 50000)  # Hookload (in pounds)
                standpipe_pressure = random.uniform(1000, 5000)  # Standpipe pressure (in psi)
                surface_pressure = random.uniform(500, 3000)  # Surface pressure (in psi)
                wellbore_trajectory = f"Inclination: {random.randint(1, 90)}°, Azimuth: {random.randint(0, 360)}°"  # Description of wellbore trajectory

                # Append each row of data to drilling_logs list
                site_logs.append({
                    'well_id': well_id,
                    'depth': depth,
                    'lithology': lithology,
                    'formation_thickness': formation_thickness,
                    'rop': rop,
                    'wob': wob,
                    "rotary_speed": rotary_speed,
                    'torque': torque,
                    'mud_flow_rate': mud_flow_rate,
                    'mud_weight': mud_weight,
                    'hookload': hookload,
                    'standpipe_pressure': standpipe_pressure,
                    'surface_pressure': surface_pressure,
                    'wellbore_trajectory': wellbore_trajectory
                })

        # convert list to dataframe
        drilling_logs[sid] = pd.DataFrame(site_logs)

        globals()[f"dl{site_index:03}"] = drilling_logs[sid]
        # save each file to new_path
        filename = os.path.join(new_path, f"drilling_logs_site_{site_index:03}.xlsx")
        drilling_logs[sid].to_excel(filename, index=False)

    return drilling_logs


def seismic_survey_simulator(surveys, records):
    """
    This function creates seismic survey data based on the number of surveys and records per survey.
    :param surveys: Determines how many dataframes the function will return for each survey.
    :param records: Determines the number of rows per survey the function will return
    :return: A number of dataframes stored as variables based on number of surveys, eg ss1, ss2, ss3, etc.
    """
    seismic_data = {}

    # Create directory to output each file
    cwd = os.getcwd()
    path_name = "seismic_survey"
    new_path = os.path.join(cwd, path_name)
    if os.path.exists(new_path):
        shutil.rmtree(new_path)
    os.makedirs(new_path, exist_ok=True)

    for survey_index in range(1, surveys+1):
        survey_data = []
        # create Survey ID for each survey
        sid = f"S{survey_index}"
        for record in range(1, records+1):
            # Create variables from survey
            survey_id = sid  # Unique identifier for each seismic survey
            location = f"{fake.latitude()}, {fake.longitude()}"  # Coordinates of the survey area
            shot_point_id = random.choice(["SP1", "SP2", "SP3"])
            receiver_point_id = random.choice(["RP1", "RP2", "RP3"])
            seismic_reflection_data = f"Reflection Amplitude: {round(random.random(), 1)}"

            # Append data for each survey location
            survey_data.append({
                "survey_id": survey_id,
                "location": location,
                "shot_point_id": shot_point_id,
                "receiver_point_id": receiver_point_id,
                "seismic_reflection_data": seismic_reflection_data
            })

        # Convert the list to a dataframe that would be stored in seismic data dictionary
        seismic_data[sid] = pd.DataFrame(survey_data)

        # Create variables for each Dataframe to be stored in e.g ss1, ss2, etc
        globals()[f"{sid}"] = seismic_data[sid]

        # save each file to new_path
        filename = os.path.join(new_path, f"seismic_survey_{survey_index}.xlsx")
        seismic_data[sid].to_excel(filename, index=False)

    return seismic_data


def production_data_simulator(sites, wells, records):
    """
    This function generates production data from several wells across several sites
    :param sites: Determines number of sites and Dataframes generated
    :param wells: Determines the number of wells per site
    :param records: Determines the volume of data recorded for each well.
    :return: Returns Dataframes of productions data for each site.
    """

    # Create empty dictionary to store each dataframe from each site
    production_data = {}

    # Create directory to output each file
    cwd = os.getcwd()
    path_name = "production_data"
    new_path = os.path.join(cwd, path_name)
    if os.path.exists(new_path):
        shutil.rmtree(new_path)
    os.makedirs(new_path, exist_ok=True)

    for site_index in range(1, sites + 1):
        site_data = []
        # Create site id
        sid = f"S{site_index:03}"
        for well_index in range(1, wells+1):
            wid = f"{sid}-{well_index:03}"
            for record in range(1, records+1):
                well_id = wid  # Unique identifier for each production well
                production_date = fake.date()  # Date of production data record
                production_volume = random.randint(500, 3500)  # Amount of crude produced in barrel
                equipment_status = f"Pump: {random.choice(['Operational', 'Maintenance', 'Repair', 'Shutdown', 'Standby', 'Testing', 'Idle', 'Out of Service'])}"  # Status of production equipment
                environmental_status = "Compliance: Yes"

                # Append information to list
                site_data.append({
                    "well_id": well_id,
                    "production_date": production_date,
                    "production_volume": production_volume,
                    "equipment_status": equipment_status,
                    "environmental_status": environmental_status
                })
            # Convert each site data to data frame and store in dictionary
            production_data[sid] = pd.DataFrame(site_data)

            # Save each production data to a variable e.g pd001, pd002, etc
            globals()[f"pd{sid}"] = production_data[sid]

            # save each file to new_path
            filename = os.path.join(new_path, f"production_data_site_{site_index:03}.xlsx")
            production_data[sid].to_excel(filename, index=False)

    return production_data


def personnel_information_simulator(staffs):
    """
    This function generates personnel information for Horizon Energy Exploration Corporation in JSON file format
    :param staffs: Determines how many personnel information is going to be generated
    :return: A JSON document of personnel information
    """
    personnel = []

    # Create directory to output each file
    cwd = os.getcwd()
    path_name = "personnel_data"
    new_path = os.path.join(cwd, path_name)
    if os.path.exists(new_path):
        shutil.rmtree(new_path)
    os.makedirs(new_path, exist_ok=True)
    for staff_index in range(1, staffs+1):
        staff = {"personnel_id": f"{staff_index:05}",
                 "name": f"{fake.name()}",
                 "job_title": f"{random.choice(['Geologist', 'Petroleum Engineer', 'Drilling Engineer', 'Seismic Interpreter', 'Environmental Specialist'])}",
                 "qualification": ["Drilling Certification", "Safety Training"],
                 "date_of_birth": f"{fake.date()}",
                 "email": f"{fake.email()}",
                 "city": f"{fake.city()}",
                 "address": f"{fake.address()}",
                 "salary": random.randint(70000, 120000),
                 "phone_number": f"{fake.phone_number()}"
                 }
        # Append each dictionary/document to list
        personnel.append(staff)
    # Convert list to JSON document
    json_data = json.dumps(personnel, indent=4)
    # Output JSON data to directory
    filename = os.path.join(new_path, "personnel_data.json")
    with open(filename, "w") as file:
        file.write(json_data)

    return json_data


# Call functions to generate data
drilling_logs_simulator(10, 5, 150)
seismic_survey_simulator(10, 100)
production_data_simulator(10, 9, 100)
personnel_information_simulator(3000)


