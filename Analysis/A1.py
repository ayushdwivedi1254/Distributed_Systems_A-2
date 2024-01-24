import asyncio
import aiohttp
import matplotlib.pyplot as plt
import json

server_dict = {}


async def make_request(session, url):
    async with session.get(url) as response:
        res = await response.text()
        return res


async def generate_requests():
    url = "http://localhost:5000/home"

    async with aiohttp.ClientSession() as session:
        tasks = [make_request(session, url) for _ in range(10000)]
        responses = await asyncio.gather(*tasks)

    return responses


def parse(result):
    global server_dict
    for response in result:
        parsed_response = json.loads(response)
        data_value = parsed_response.get("data", "")
        server_id = data_value.split(":")[-1].strip()

        if server_id in server_dict:
            server_dict[server_id] += 1
        else:
            server_dict[server_id] = 1
    print(server_dict)


def plot_bar_chart(data_dict, title, xlabel, ylabel):
    keys = list(data_dict.keys())
    values = list(data_dict.values())

    bar_color = 'skyblue'

    plt.bar(keys, values, color=bar_color, edgecolor='black', linewidth=1.2)
    plt.title(title, fontsize=16)
    plt.xlabel(xlabel, fontsize=14)
    plt.ylabel(ylabel, fontsize=14)

    # Add labels to each bar
    for key, value in zip(keys, values):
        plt.text(key, value + 0.1, str(value),
                 ha='center', va='bottom', fontsize=12)

    # Customize the grid and axis ticks
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)

    # Display the plot
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(generate_requests())
    # for response in result:
    #     print(response)
    parse(result)
    ordered_dict = {key: server_dict[key] for key in sorted(server_dict)}
    chart_title = "Requests per Server"
    x_axis_label = "Server ID"
    y_axis_label = "Number of Requests"

    # Call the plot function
    plot_bar_chart(ordered_dict, chart_title, x_axis_label, y_axis_label)
