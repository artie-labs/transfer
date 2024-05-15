---
description: >-
  In this guide, we will walk through what is a SSH tunnel and how you can
  enable this within Artie.
---

# Enabling SSH Tunneling

## What is SSH Tunneling?

An SSH tunnel refers to a secured connection between a local machine and a remote server. SSH tunnels are a great and secure way to manage access to private resources within your VPC.

With Artie, it would look something like this:

<figure><img src="../.gitbook/assets/image (17).png" alt="" width="563"><figcaption></figcaption></figure>

### Step 1 - Creating a SSH tunnel

If you have one already, skip this step. However, if you don't - then make sure to create an EC2 or equivalent instance. Make sure this instance can connect to your database.

### Step 2 - Allowing Artie to connect

* Create a new SSH Tunnel from our dashboard
* Grab the public IP address and port

<figure><img src="../.gitbook/assets/image (11).png" alt="" width="563"><figcaption><p>AWS console to grab the IP address</p></figcaption></figure>

* Once the SSH tunnel is created, copy the public key and add it to your server

{% hint style="info" %}
You can create the SSH tunnel from [**Company Settings**](https://app.artie.com/settings) or under **Advanced Settings** when you edit a deployment!
{% endhint %}

<figure><img src="../.gitbook/assets/image (14).png" alt="" width="563"><figcaption><p>Artie advanced settings</p></figcaption></figure>

```bash
# SSH into your instance

# (Optional) if you want to create a service account
sudo adduser -m artie
sudo su artie
mkdir ~/.ssh

# Add the public key to ~/.ssh/authorized_keys
vi ~/.ssh/authorized_keys

chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys

exit
```
