# smoke_test.py
import asyncio
from fastmcp import Client


async def main() -> None:
    async with Client("http://localhost:8000/mcp") as client:
        tools = await client.list_tools()
        print("tools:", [t.name for t in tools])

        topics = await client.call_tool("list_topics", {})
        print("topics:", topics.data)

        peek = await client.call_tool("peek_topic", {"topic": "catch-log", "limit": 3})
        print(f"peek catch-log (count={peek.data['count']}):")
        for record in peek.data["records"]:
            print(f"  {record['key']!r:<14} -> {record['value']}")

        quote = await client.call_tool(
            "quote_upgrade",
            {
                "room": "second-floor",
                "justification": "I need more space for my furniture collection.",
            },
        )
        print("quote:", quote.data)


asyncio.run(main())
