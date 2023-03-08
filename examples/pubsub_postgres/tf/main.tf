# In here, we will be doing the following:
# 1) Creating a service account
# 2) Creating a topic
# 3) Creating a subscription
# Note: - We require a topic per table that we want to replicate from.
locals {
  project = "artie-labs"
  role = "roles/pubsub.editor"
}

provider "google" {
  project     = local.project
  // Authenticate via gcloud auth application-default login
  // This requires the gcloud CLI downloaded: https://cloud.google.com/sdk/docs/install
  // Need to enable PubSub API: https://console.cloud.google.com/marketplace/product/google/pubsub.googleapis.com
}

resource "google_service_account" "artie-svc-account" {
  account_id   = "artie-service-account"
  display_name = "Service Account for Artie Transfer and Debezium"
}

resource "google_project_iam_member" "transfer" {
  project = local.project
  role    = local.role
  member  = "serviceAccount:${google_service_account.artie-svc-account.email}"
}

# Pub/Sub configurations
# We will create the topic and subscription.
resource "google_pubsub_topic" "customer_topic" {
  name    = "dbserver1.inventory.customers"
  project = local.project

  timeouts {}
}

resource "google_pubsub_subscription" "customer_subscription" {
  ack_deadline_seconds         = 300
  enable_exactly_once_delivery = false
  enable_message_ordering      = true
  message_retention_duration   = "604800s"
  name                         = "transfer_${google_pubsub_topic.customer_topic.name}"
  project                      = local.project
  retain_acked_messages        = false
  topic                        = google_pubsub_topic.customer_topic.id

  timeouts {}
}
