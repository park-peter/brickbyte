# Databricks notebook source
# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC # Microsoft Teams to Databricks with BrickByte
# MAGIC 
# MAGIC This notebook syncs data from Microsoft Teams to Delta Lake tables in Unity Catalog.
# MAGIC 
# MAGIC **Available streams:**
# MAGIC - `users` - Organization users
# MAGIC - `groups` - Microsoft 365 groups
# MAGIC - `group_members` - Group membership
# MAGIC - `teams` - Teams in your organization
# MAGIC - `team_members` - Team membership
# MAGIC - `channels` - Channels within teams
# MAGIC - `channel_members` - Channel membership
# MAGIC - `channel_messages` - Messages in channels
# MAGIC - `channel_tabs` - Tabs in channels
# MAGIC - `conversations` - Group conversations
# MAGIC - `conversation_threads` - Conversation threads
# MAGIC - `conversation_posts` - Posts in conversations
# MAGIC - `team_device_usage_report` - Device usage reports
# MAGIC - `team_drives` - Team drives
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - Azure AD App Registration with Microsoft Graph API permissions
# MAGIC - Client ID, Client Secret, and Tenant ID

# COMMAND ----------

from brickbyte import BrickByte

bb = BrickByte()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Azure AD App Setup
# MAGIC 
# MAGIC 1. Go to [Azure Portal](https://portal.azure.com/) → Azure Active Directory → App registrations
# MAGIC 2. Click **New registration**, name it (e.g., "BrickByte Teams Connector")
# MAGIC 3. Under **API permissions**, add Microsoft Graph **Application permissions**:
# MAGIC    - `Group.Read.All`
# MAGIC    - `Channel.Read.All`
# MAGIC    - `Team.ReadBasic.All`
# MAGIC    - `User.Read.All`
# MAGIC    - `ChannelMessage.Read.All` (for messages)
# MAGIC 4. Click **Grant admin consent**
# MAGIC 5. Under **Certificates & secrets**, create a new client secret
# MAGIC 6. Copy: **Client ID**, **Tenant ID** (from Overview), and **Client Secret**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1: Auto-Discovered Credentials (Recommended)
# MAGIC 
# MAGIC Set up secrets once:
# MAGIC ```bash
# MAGIC databricks secrets create-scope brickbyte
# MAGIC databricks secrets put-secret brickbyte source-microsoft-teams/tenant_id
# MAGIC databricks secrets put-secret brickbyte source-microsoft-teams/client_id
# MAGIC databricks secrets put-secret brickbyte source-microsoft-teams/client_secret
# MAGIC ```
# MAGIC 
# MAGIC Credentials are auto-discovered and merged into the config!

# COMMAND ----------

# Credentials auto-discovered - just provide non-sensitive config
result = bb.sync(
    source="source-microsoft-teams",
    source_config={
        "credentials": {
            "auth_type": "Client",
            # tenant_id, client_id, client_secret auto-discovered from secrets!
        },
        "period": "D7",
    },
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
)

print(f"Synced {result.records_written} records from {len(result.streams_synced)} streams")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2: Inline Credentials (Quick Testing)

# COMMAND ----------

# Your Azure AD app credentials (for testing only)
client_id = "YOUR_CLIENT_ID"
client_secret = "YOUR_CLIENT_SECRET"
tenant_id = "YOUR_TENANT_ID"

result = bb.sync(
    source="source-microsoft-teams",
    source_config={
        "credentials": {
            "auth_type": "Client",
            "client_id": client_id,
            "client_secret": client_secret,
            "tenant_id": tenant_id,
        },
        "period": "D7",  # Last 7 days for reports
    },
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
)

print(f"Synced {result.records_written} records from {len(result.streams_synced)} streams")
print(f"Streams: {result.streams_synced}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync Specific Streams Only

# COMMAND ----------

# Sync only teams and channels
result = bb.sync(
    source="source-microsoft-teams",
    source_config={
        "credentials": {
            "auth_type": "Client",
            "client_id": client_id,
            "client_secret": client_secret,
            "tenant_id": tenant_id,
        },
        "period": "D7",
    },
    streams=["teams", "channels", "team_members"],
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
)

print(f"Synced {result.records_written} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync Users and Groups

# COMMAND ----------

result = bb.sync(
    source="source-microsoft-teams",
    source_config={
        "credentials": {
            "auth_type": "Client",
            "client_id": client_id,
            "client_secret": client_secret,
            "tenant_id": tenant_id,
        },
        "period": "D30",  # Last 30 days
    },
    streams=["users", "groups", "group_members"],
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
)

print(f"Synced {result.records_written} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync Channel Messages
# MAGIC 
# MAGIC **Note:** Requires `ChannelMessage.Read.All` permission.

# COMMAND ----------

result = bb.sync(
    source="source-microsoft-teams",
    source_config={
        "credentials": {
            "auth_type": "Client",
            "client_id": client_id,
            "client_secret": client_secret,
            "tenant_id": tenant_id,
        },
        "period": "D7",
    },
    streams=["channel_messages"],
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
)

print(f"Synced {result.records_written} messages")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync Conversations (Group Conversations)

# COMMAND ----------

result = bb.sync(
    source="source-microsoft-teams",
    source_config={
        "credentials": {
            "auth_type": "Client",
            "client_id": client_id,
            "client_secret": client_secret,
            "tenant_id": tenant_id,
        },
        "period": "D7",
    },
    streams=["conversations", "conversation_threads", "conversation_posts"],
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
)

print(f"Synced {result.records_written} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync Usage Reports

# COMMAND ----------

result = bb.sync(
    source="source-microsoft-teams",
    source_config={
        "credentials": {
            "auth_type": "Client",
            "client_id": client_id,
            "client_secret": client_secret,
            "tenant_id": tenant_id,
        },
        "period": "D30",  # Reports for last 30 days
    },
    streams=["team_device_usage_report"],
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
)

print(f"Synced {result.records_written} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## With AI Metadata Enrichment
# MAGIC 
# MAGIC Auto-generate column descriptions and detect PII (e.g., email addresses, user names).

# COMMAND ----------

# result = bb.sync(
#     source="source-microsoft-teams",
#     source_config={
#         "credentials": {
#             "auth_type": "Client",
#             "client_id": client_id,
#             "client_secret": client_secret,
#             "tenant_id": tenant_id,
#         },
#         "period": "D7",
#     },
#     streams=["users", "teams", "channels"],
#     catalog="",  # TODO: Set your Unity Catalog name
#     schema="",   # TODO: Set your target schema
#     enrich_metadata=True,
#     enrich_model="databricks-meta-llama-3-3-70b-instruct",
# )

# print(f"Enriched tables: {result.enriched_tables}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Your Teams Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View synced teams
# MAGIC -- SELECT * FROM your_catalog.your_schema.teams LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View channel messages
# MAGIC -- SELECT * FROM your_catalog.your_schema.channel_messages LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Join teams with channels
# MAGIC -- SELECT t.displayName as team_name, c.displayName as channel_name
# MAGIC -- FROM your_catalog.your_schema.teams t
# MAGIC -- JOIN your_catalog.your_schema.channels c ON t.id = c.teamId
