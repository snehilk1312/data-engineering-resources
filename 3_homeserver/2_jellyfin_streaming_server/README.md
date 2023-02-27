### what does this server does:
* Creates a jellyfin server with three ports mapped to host - port 8096 open to internet for access through client.
* Creates a transmission server, meant to download contents directly on the server itself - is meant to accessed through ssh port forwarding through port 9091(don't open to public internet)
* Shared **Download** folder - here transmission downloads the content and the content instantly gets update into jellyfin clients to be consumed
