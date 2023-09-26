# Fixed IP Addresses

To ensure that Artie is able to subscribe to your sources and push data to your destination, please  add all of Artie's IP addresses below to your allowlist.

Today, we support the following cloud providers:

* Google Cloud Platform
* Amazon Web Services

{% hint style="warning" %}
Please make sure to add all the IP addresses from the **Control Plane** + **AWS US-East-1.**  By default, you will be put in the US-East-1 region, please make sure to contact support if you'd like to be moved to another region.
{% endhint %}

## Control Plane IP ranges (CIDR format)

{% hint style="info" %}
Add the following IP addresses so that the dashboard is able to validate connectivity!
{% endhint %}

* 164.90.147.255/32
* 164.90.147.217/32
* 147.182.226.70/32
* 24.199.76.121/32
* 143.198.244.128/32
* 164.92.90.51/32

## Data Plane IP Ranges (CIDR format)

### **AWS US-East-1 (Default)**

* 3.215.55.30/32
* 3.216.86.119/32
* 3.234.42.107/32
* 3.209.157.160/32
* 3.212.233.181/32
* 3.216.214.184/32
* 34.225.190.48/32
* 54.165.47.156/32
* 44.216.116.232/32
* 54.164.238.160/32

### **AWS US-West-2**

* 54.191.160.248/32
* 52.35.174.184/32
* 54.71.53.235/32
* 44.238.111.7/32
* 100.20.16.250/32
* 54.245.136.219/32
* 52.33.116.144/32
* 44.238.174.20/32
* 54.70.103.212/32
* 54.185.25.39/32
