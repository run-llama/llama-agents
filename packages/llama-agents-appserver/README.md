# llama-agents-appserver

Application server components for LlamaAgents.

For an end-to-end introduction, see [Getting started with LlamaAgents](https://developers.llamaindex.ai/python/cloud/llamaagents/getting-started).

Hosted workflows decode JSON with the source server's declared types, including
the legacy task event route. The appserver reads `WorkflowServer.json_serializer`
before transferring the workflows, so source `additional_events` keep working
after the transfer. Explicit workflow and source-server serializers are preserved
as configured. See the server README for dynamic lookup and `extra_types`.
