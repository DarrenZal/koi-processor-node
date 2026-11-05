"""URLValidator refuses every address that is not publicly routable, not only the private ranges."""
import socket

import pytest

from api.web_fetcher import URLValidationError, URLValidator


@pytest.mark.parametrize("url", [
    "http://100.64.0.1/",        # shared address space
    "http://100.127.255.254/",
    "http://224.0.0.1/",         # multicast
    "http://[ff02::1]/",
    "http://127.0.0.1/",
    "http://10.0.0.1/",
    "http://169.254.169.254/",
    "http://[::1]/",
    "http://[::ffff:127.0.0.1]/",
])
def test_non_public_literals_are_refused(url):
    with pytest.raises(URLValidationError):
        URLValidator().validate(url)


@pytest.mark.parametrize("url", ["http://93.184.216.34/", "https://[2606:2800:220:1:248:1893:25c8:1946]/"])
def test_public_literals_are_accepted(url):
    assert URLValidator().validate(url) == url


@pytest.mark.parametrize("address", ["100.64.0.1", "224.0.0.1"])
def test_a_hostname_resolving_to_a_non_public_address_is_refused(monkeypatch, address):
    answer = [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (address, 0))]
    monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: answer)
    with pytest.raises(URLValidationError):
        URLValidator().validate("http://name.invalid/")
