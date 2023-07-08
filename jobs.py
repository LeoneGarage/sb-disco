import requests
import copy

class Jobs:
  _api_url = None
  _token = None

  def __init__(self, api_url, token):
    self._api_url = api_url
    self._token = token

  def send_job_request(self, action, request_func):
    api_url = self._api_url
    token = self._token
    
    response = request_func(f'{api_url}/api/2.1/jobs/{action}', {"Authorization": f"Bearer {token}"})
    if response.status_code != 200:
      raise Exception("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
    return response.json()

  def find_job_by_name(self, name):
    jobs = self.send_job_request('list', lambda u, h: requests.get(u, headers=h))
    j = [job for job in jobs['jobs'] if job['settings'].get('name', '') == name]
    if len(j) > 0:
      return j[0]
    return None

  def get_job_by_name(self, name):  
    job = self.find_job_by_name(name)
    if job is None:
      raise Exception(f'Job with name {name} not found')

    job_id = job['job_id']

    job = self.send_job_request('get', lambda u, h: requests.get(f'{u}&job_id={job_id}', headers=h))
    return job

  def reset_job_by_name(self, name, job_config):
    j = self.get_job_by_name(name)
    response = self.send_job_request('reset', lambda u, h: requests.post(f'{u}', json=job_config, headers=h))
    return response

  def create_python_job(self,
                        job_name,
                        bootstrap_copy_notebook_path,
                        source_zip,
                        dest_zip,
                        git_url,
                        git_provider="gitHub",
                        git_branch="main",
                        python_file,
                        parameters=None,
                        min_workers = None,
                        max_workers = None,
                        spark_conf=None,
                        libraries=None,
                        packages=None,
                        instance_profile_arn=None):
    if libraries is not None:
      libraries = [{"whl":lib} for lib in libraries]
    else:
      libraries = []
    if packages is not None:
      packages = [{"pypi":{"package":p}} for p in packages]
    else:
      packages = []
    if min_workers is None:
      min_workers = 2
    if max_workers is None:
      max_workers = 64
    job_config = {
      "name":f"{job_name}",
      "email_notifications":{
        "no_alert_for_skipped_runs":False
      },
      "webhook_notifications":{},
      "timeout_seconds":0,
      "max_concurrent_runs":1,
      "tasks":[
        {"task_key":"bootstrap_copy",
        "notebook_task": {
          "notebook_path": f"{bootstrap_copy_notebook_path}",
          "source": "GIT",
          "base_parameters": {
            "source": f"{source_zip}",
            "dest": f"{dest_zip}"
          }
        },
        "job_cluster_key":f"{job_name}_cluster",
        "timeout_seconds":0,
        "email_notifications":{}
        },
        {"task_key":f"{job_name}",
        "spark_python_task":{
          "python_file":python_file,
          "parameters":parameters
        },
        "depends_on":{
          "task_key": "bootstrap"
        },
        "libraries":libraries + packages,
        "job_cluster_key":f"{job_name}_cluster",
        "timeout_seconds":0,
        "email_notifications":{}
        }],
      "job_clusters":[
        {"job_cluster_key":f"{job_name}_cluster",
        "new_cluster":{
          "cluster_name":"",
          "spark_version":"13.2.x-scala2.12",
          "spark_conf": spark_conf,
          "aws_attributes":{
            "first_on_demand":1,
            "availability":"SPOT_WITH_FALLBACK",
            "zone_id":"auto",
            "instance_profile_arn":instance_profile_arn,
            "spot_bid_price_percent":100,
            "ebs_volume_type":"GENERAL_PURPOSE_SSD",
            "ebs_volume_count":3,
            "ebs_volume_size":100
          },
          "node_type_id":"i3.xlarge",
          "driver_node_type_id":"m5.xlarge",
          "enable_elastic_disk":False,
          "data_security_mode":"NONE",
          "runtime_engine":"STANDARD",
          "autoscale": {
            "min_workers":f"{min_workers}",
            "max_workers":f"{max_workers}"
          },
          "num_workers":f"{max_workers}"
        }
        }],
        "git_source": {
            "git_url": f"{git_url}",
            "git_provider": f"{git_provider}",
            "git_branch": f"{git_branch}"
        }
      "format":"MULTI_TASK"
    }
    if min_workers == max_workers:
      del job_config['job_clusters'][0]['new_cluster']['autoscale']
    else:
      del job_config['job_clusters'][0]['new_cluster']['num_workers']
    j = self.find_job_by_name(job_name)
    if j is not None:
      job_id = j['job_id']
      job_config = {
        "job_id": job_id,
        "new_settings": job_config
      }
      print(job_config)
      response = self.send_job_request('reset', lambda u, h: requests.post(f'{u}', json=job_config, headers=h))
    else:
      print(job_config)
      response = self.send_job_request('create', lambda u, h: requests.post(f'{u}', json=job_config, headers=h))
    return response