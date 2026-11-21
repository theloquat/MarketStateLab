"""Validation for the research planner connection configuration."""

import ipaddress
from urllib.parse import urlsplit

def normalize(values: dict, require_model=False) -> dict:
    base = values.get("base_url", "https://api.openai.com/v1")
    if not isinstance(base, str) or not base.strip() or len(base) > 2000 or any(c.isspace() for c in base) or "\\" in base:
        raise ValueError("模型 API 地址无效")
    base = base.strip().rstrip("/")
    try:
        parsed = urlsplit(base)
        host, port = parsed.hostname, parsed.port
    except ValueError:
        raise ValueError("模型 API 地址无效") from None
    if not host or parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment or (port is not None and not 1 <= port <= 65535):
        raise ValueError("模型 API 地址不能包含认证信息、查询参数或片段")
    loopback = host.lower() == "localhost"
    try:
        loopback = loopback or ipaddress.ip_address(host).is_loopback
    except ValueError:
        pass
    if parsed.scheme != "https" and not (parsed.scheme == "http" and loopback):
        raise ValueError("模型服务需使用 HTTPS，或本机回环地址的 HTTP")
    if parsed.path.rstrip("/").endswith(("/chat/completions", "/responses")):
        raise ValueError("请填写 API 根地址，不要包含 /chat/completions 或 /responses")
    model = values.get("model", "")
    key = values.get("api_key", "")
    if not isinstance(model, str) or len(model) > 200 or any(ord(c) < 32 for c in model):
        raise ValueError("模型名称无效")
    if not isinstance(key, str) or len(key) > 4096 or any(c.isspace() for c in key):
        raise ValueError("API Key 格式无效")
    if require_model and not model.strip():
        raise ValueError("请填写服务商提供的模型 ID")
    limit = values.get("max_calls_per_day", 10)
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("每日调用次数需为 1 至 1000 的整数")
    parameter = values.get("token_parameter", "max_completion_tokens")
    if parameter not in ("max_completion_tokens", "max_tokens"):
        raise ValueError("不支持的模型输出长度参数")
    return {"base_url": base, "model": model.strip(), "api_key": key,
            "max_calls_per_day": limit, "token_parameter": parameter}
