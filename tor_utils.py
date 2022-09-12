from pathlib import Path
import shutil

from stem import Signal
from stem.control import Controller
import stem.process
import requests

tor_path = shutil.which("tor")
if not tor_path:
    raise Exception("It is mandatory to install 'tor' first")


class Tor:
    def __init__(self, socks_port, exit_nodes=None):
        # Destroy any previous temporary data directory
        self.socks_port = socks_port
        self.exit_nodes = exit_nodes
        self.session = None
        self.tor_process = None
        self.renew_connection()

    def renew_connection(self):
        print("Renewing tor circuit, this could take some time. Please be patient")
        if self.tor_process:
            self.tor_process.kill()
        data_directory = Path(f"/tmp/tor/{self.socks_port}")
        data_directory.mkdir(parents=True, exist_ok=True)
        tor_config = {
            "SocksPort": str(self.socks_port),
            "DataDirectory": data_directory.as_posix(),
        }
        if self.exit_nodes:
            tor_config.update({"ExitNodes": f"{{{self.exit_nodes}}}"})
        self.tor_process = stem.process.launch_tor_with_config(
            tor_cmd=tor_path, take_ownership=True, config=tor_config
        )

        self.session = requests.session()
        # Tor uses the 9050 port as the default socks port
        self.session.proxies = {
            "http": f"socks5://127.0.0.1:{self.socks_port}",
            "https": f"socks5://127.0.0.1:{self.socks_port}",
        }
        print("Connection renewed!")
