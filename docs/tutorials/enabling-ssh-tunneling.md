---
description: >-
  In this guide, we'll go over how to set up SSH tunneling so that Artie can
  connect to your database without going over the internet.
---

# Enabling SSH Tunneling

## How this looks

<figure><img src="../.gitbook/assets/image (4).png" alt=""><figcaption></figcaption></figure>

### Setting up bastion host

If you have one already, skip this step. However, if you don't - then make sure to create an EC2 or equivalent instance. Make sure this instance can connect to your database!

### Allowing Artie to connect

1. SSH into your newly created instance
2. Find your company's SSH public key (Artie Dashboard) and add it to `~/.ssh/authorized_keys` \[[AWS guide](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/replacing-key-pair.html)]
3. Grab the public IP address, port and username and add this to your company [advanced settings](https://app.artie.so/settings) on the Artie dashboard.&#x20;

<figure><img src="../.gitbook/assets/image (3).png" alt=""><figcaption><p>Artie advanced settings</p></figcaption></figure>

<figure><img src="../.gitbook/assets/image (2).png" alt=""><figcaption><p>AWS console to grab the IP address</p></figcaption></figure>
