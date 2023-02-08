import os
import shutil
import signal
import sys
import tempfile
import time
from subprocess import Popen, PIPE

# --------------------------------------------------------------------------------
# Constant
# --------------------------------------------------------------------------------
SPARK_HOME = "/opt/homebrew/Cellar/apache-spark/3.3.1/libexec"
JAVA_HOME = '/opt/homebrew/opt/openjdk'

# --------------------------------------------------------------------------------
# Environment Variables
# NOTE:
# SPARK_HOME must be set to /opt/homebrew/Cellar/apache-spark/3.3.1/libexec",
# NOT /opt/homebrew/Cellar/apache-spark/3.3.1".
# Otherwise Java gateway process exited before sending its port number in java_gateway.py
# --------------------------------------------------------------------------------
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ['JAVA_HOME'] = JAVA_HOME
sys.path.extend([
    f"{SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip",
    f"{SPARK_HOME}/python/lib/pyspark.zip",
])


# --------------------------------------------------------------------------------
# Pyspark Modules
# --------------------------------------------------------------------------------
from pyspark.serializers import read_int, UTF8Deserializer


# --------------------------------------------------------------------------------
#
# --------------------------------------------------------------------------------
def preexec_func():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def run_pyspark():
    """
    """
    pyspark_command = [f'{SPARK_HOME}/bin/spark-submit', '--master', 'local[*]', 'pyspark-shell']

    # Create a temporary directory where the gateway server should write the connection
    # information.
    proc = None
    conn_info_dir = tempfile.mkdtemp()
    try:
        fd, conn_info_file = tempfile.mkstemp(dir=conn_info_dir)
        os.close(fd)
        os.unlink(conn_info_file)

        env = dict(os.environ)
        env["_PYSPARK_DRIVER_CONN_INFO_PATH"] = conn_info_file

        # Launch the Java gateway.
        popen_kwargs = {"stdin": PIPE, "env": env}
        # We open a pipe to stdin so that the Java gateway can die when the pipe is broken
        # We always set the necessary environment variables.

        print(f"\nrun pyspark command line {pyspark_command}")
        popen_kwargs["preexec_fn"] = preexec_func
        proc = Popen(pyspark_command, **popen_kwargs)

        # Wait for the file to appear, or for the process to exit, whichever happens first.
        count: int = 5
        while not proc.poll() and not os.path.isfile(conn_info_file):
            print("waiting for pyspark to start...")
            count -= 1
            if count < 0:
                break

            time.sleep(1)

        if not os.path.isfile(conn_info_file):
            raise RuntimeError("Java gateway process exited before sending its port number")

        with open(conn_info_file, "rb") as info:
            gateway_port = read_int(info)
            gateway_secret = UTF8Deserializer().loads(info)

        out, err = proc.communicate()
        print("-"*80)
        print(f"pyspark started with pid {proc.pid}")
        print(f"spark process port {gateway_port}")
    finally:
        shutil.rmtree(conn_info_dir)
        if proc:
            proc.kill()


def test():
    run_pyspark()


if __name__ == "__main__":
    test()