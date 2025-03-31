import torch


def get_torch_devices():
    """
    List all devices available for PyTorch
    """
    devices = ["cpu"]

    # CUDE
    if torch.cuda.is_available():
        for i in range(torch.cuda.device_count()):
            devices.append(f"cuda:{i}")

    # MPS
    if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
        devices.append("mps")

    return devices
