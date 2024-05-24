## Furiko: Kubernetes-native job platform Installation
---
### 1. Furiko Resource Deployment
```bash
kubectl apply -f https://github.com/furiko-io/furiko/releases/latest/download/furiko-execution.yaml
```

### 2. CLI Install & Check
```bash
wget https://github.com/furiko-io/furiko/releases/download/v0.2.0/furiko_linux_amd64
sudo mv furiko_darwin_amd64 /usr/local/bin/furiko
sudo chmod +x /usr/local/bin/furiko

furiko --help

    Command-line utility to manage Furiko.

    Usage:
      furiko [command]

    Available Commands:
      completion  generate the autocompletion script for the specified shell
      disable     Disable automatic scheduling for a JobConfig.
      enable      Enable automatic scheduling for a JobConfig.
      get         Get one or more resources by name.
      help        Help about any command
      kill        Kill an ongoing Job.
      list        List all resources by kind.
      run         Run a new Job.

    Flags:
          --dynamic-config-name string        Overrides the name of the dynamic cluster config. (default "execution-dynamic-config")
          --dynamic-config-namespace string   Overrides the namespace of the dynamic cluster config. (default "furiko-system")
      -h, --help                              help for furiko
          --kubeconfig string                 Path to the kubeconfig file to use for CLI requests.
      -n, --namespace string                  If present, the namespace scope for this CLI request.
      -v, --v int                             Sets the log level verbosity.

    Use "furiko [command] --help" for more information about a command.
```