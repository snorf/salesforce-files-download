import threading
import json
import os

class DeployReporter:
    def __init__(self, json_path='deploy_summary.json'):
        self.json_path = json_path
        self.lock = threading.Lock()
        # Try to load existing data for append mode
        if os.path.exists(self.json_path):
            with open(self.json_path, 'r') as f:
                try:
                    self.data = json.load(f)
                except Exception:
                    self.data = {}
        else:
            self.data = {}

    def log(self, object_name, category):
        with self.lock:
            if object_name not in self.data:
                self.data[object_name] = {}
            if category not in self.data[object_name]:
                self.data[object_name][category] = 0
            self.data[object_name][category] += 1
            self._save()

    def _save(self):
        with open(self.json_path, 'w') as f:
            json.dump(self.data, f, indent=2)

    def get_summary(self):
        # Returns dict: {object: {type: count, ...}, ...}
        return self.data

    def clear(self):
        self.data = {}
        if os.path.exists(self.json_path):
            os.remove(self.json_path)

reporter = DeployReporter()
