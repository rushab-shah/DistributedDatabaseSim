## Author: Rushab Shah, Deepali Chugh
## File: incident.py
## Date: 12/03/2022
## Purpose: This class is used for modelling a site incident. Incident can be of type fail or recovery

class Incident:
    def __init__(self, site_number, type, time) -> None:
        self.site_number = site_number
        self.incident_type = type
        self.time_of_occurence = time