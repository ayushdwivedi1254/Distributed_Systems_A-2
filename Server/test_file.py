import asyncio
import aiohttp


async def make_request(session, url):
    async with session.get(url) as response:
        return await response.text()


async def generate_requests():
    # Replace with the actual address of your load balancer
    url = "http://localhost:5000/home"

    async with aiohttp.ClientSession() as session:
        tasks = [make_request(session, url) for _ in range(10000)]
        responses = await asyncio.gather(*tasks)

    return responses

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(generate_requests())
    for response in result:
        print(response)
    # print(result)  # Print the first 5 responses for verification
