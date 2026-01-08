#  Copyright 2026 Ram Narayanan
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import yaml
import os
import time
from titan_sdk import TitanJob

class TitanYAMLParser:
    def __init__(self, yaml_file):
        if not os.path.exists(yaml_file):
            raise FileNotFoundError(f"YAML file not found: {yaml_file}")
            
        with open(yaml_file, 'r') as f:
            self.config = yaml.safe_load(f)
            
        # We need the root folder of the YAML file to know what to zip
        self.project_base = os.path.dirname(os.path.abspath(yaml_file))

    def get_project_name(self):
        return self.config.get('name', 'titan_agent')
    
    def is_project_mode(self):
        return self.config.get('project', True)

    def build_jobs(self):
        jobs = []
        project_name = self.get_project_name()
        zip_name = f"{project_name}.zip"
        yaml_jobs = self.config.get('jobs', [])
        is_archive = self.is_project_mode()
        
        for step in yaml_jobs:
            job_id = step.get('id')
            if not job_id: raise ValueError("Job missing ID")

            step_type = step.get('type', 'run').lower()
            relative_file = step.get('file')
            req = step.get('requirement', 'GENERAL')
            if req is None:
                req = "GENERAL"
            
            priority = int(step.get('priority', 1))
            
            # Ms to Sec conversion
            delay_sec = int(step.get('delay', 0))
            delay_ms = delay_sec * 1000 
            
            affinity = bool(step.get('affinity', False))

            if not relative_file: raise ValueError(f"Job '{job_id}' missing file")

            if is_archive:
                # ARCHIVE MODE: Pointer to zip entry
                # "my_agent.zip/src/script.py"
                final_filename = f"{zip_name}/{relative_file}".replace("\\", "/")
            else:
                # SINGLE FILE MODE: Absolute path to local file
                # "C:/Users/Titan/agent/script.py"
                final_filename = os.path.join(self.project_base, relative_file)
            
            # file_pointer = f"{zip_name}/{relative_file}".replace("\\", "/")
            
            if step_type == 'service':
                titan_type = "SERVICE"
                job_port = step.get('port')
                if not job_port: raise ValueError(f"Service '{job_id}' missing port")
            else:
                titan_type = "TASK"
                job_port = 0
            
            job = TitanJob(
                job_id=job_id,
                filename=final_filename,
                job_type=titan_type,
                args=str(step.get('args', "")),
                port=int(job_port),
                parents=step.get('depends_on', []),
                is_archive=is_archive,
                priority=int(step.get('priority', 1)),
                delay=delay_ms,
                affinity=bool(step.get('affinity', False)),
                requirement=req
            )
            jobs.append(job)
            
        return jobs