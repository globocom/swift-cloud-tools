import paramiko

USER = ""
PWD = ""


def get_load_averages():
    hostnames = ["s3devfe1.cmal08fe-1359.cp.globoi.com",
                 "s3devfe2.cmal08fe-1359.cp.globoi.com"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.load_system_host_keys()
    load_averages = []

    for host in hostnames:
        client.connect(host, username=USER, password=PWD)
        stdin, stdout, stderr = client.exec_command("cat /proc/loadavg")
        out = stdout.read().decode("utf-8")
        load_averages.append({
            "hostname": host,
            "load": out.split(" ")[:3]
        })

    return load_averages


if __name__ == "__main__":
    load = get_load_averages()
    print(load)
