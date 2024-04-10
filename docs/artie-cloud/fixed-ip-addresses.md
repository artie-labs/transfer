---
description: >-
  To ensure all of our services are able to connect to your data sources, please
  ensure you add Artie's IP addresses to your firewall's allowlist.
---

# Fixed IP Addresses

## How is Artie deployed?

We leverage a split plane architecture:

* Our control plane houses our API server.
* Our data plane performs the actual data transfer.

When you add IPs, please add both the **control plane** and your **data plane**. Need help? Check out [#which-data-plane-is-my-account-in](fixed-ip-addresses.md#which-data-plane-is-my-account-in "mention")

## Control Plane IP ranges (CIDR format)

```
52.55.119.205/32
54.204.194.211/32
52.200.253.111/32
52.44.73.206/32
44.217.73.84/32
```

## Data Plane IP Ranges (CIDR format)

### **AWS US-East-1 (Default)**

```
3.215.55.30/32
3.216.86.119/32
3.234.42.107/32
3.209.157.160/32
3.212.233.181/32
3.216.214.184/32
34.225.190.48/32
54.165.47.156/32
44.216.116.232/32
54.164.238.160/32
```

### **AWS US-West-2**

```
54.191.160.248/32
52.35.174.184/32
54.71.53.235/32
44.238.111.7/32
100.20.16.250/32
54.245.136.219/32
52.33.116.144/32
44.238.174.20/32
54.70.103.212/32
54.185.25.39/32
```

## Questions

### Which data plane is my account in?

You can see which data plane you are in by going to [https://app.artie.so/settings](https://app.artie.so/settings) and see `Data Processing Location` under `Advanced Settings`.

<figure><img src="../.gitbook/assets/image (44).png" alt="" width="563"><figcaption></figcaption></figure>

### Why is there so many IPs?

We're sorry! When we first launched, we weren't able to secure a CIDR range. We have now worked this out with AWS and will be using CIDR ranges for the next data plane we set up.

### I am hosted in another region

Get in touch with us either through our Slack or email. We'll be happy to set up another region.
