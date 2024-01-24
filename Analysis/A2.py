import asyncio
import aiohttp
import matplotlib.pyplot as plt
import json
import statistics
import requests
import time
main_dict = {}


async def make_request(session, url):
    async with session.get(url) as response:
        res = await response.text()
        return res


async def generate_requests():
    global main_dict
    global avg_load_dict
    total_requests = 10000
    load_data = []
    url = "http://localhost:5000/home"

    for n in range(2, 7):

        async with aiohttp.ClientSession() as session:
            tasks = [make_request(session, url) for _ in range(total_requests)]
            responses = await asyncio.gather(*tasks)
        main_dict[n] = parse(responses)

        # add another server
        payload = {
            'n': 1,
            'hostnames': []
        }
        response = requests.post(
            "http://localhost:5000/add", json=payload)


def parse(result):
    server_dict = {}
    for response in result:
        parsed_response = json.loads(response)
        data_value = parsed_response.get("data", "")
        server_id = data_value.split(":")[-1].strip()

        if server_id in server_dict:
            server_dict[server_id] += 1
        else:
            server_dict[server_id] = 1
    s_dict = {key: server_dict[key] for key in sorted(server_dict)}
    print(s_dict)
    return s_dict


def calculate_average_load_and_std_dev():
    global main_dict
    average_loads = {}
    std_devs = {}

    for n in range(2, 7):
        total_requests = 0
        nums = []
        for key in main_dict[n]:
            value = main_dict[n][key]
            total_requests += value
            nums.append(value)

        average_loads[n] = total_requests / n
        std_devs[n] = statistics.stdev(nums)

    return average_loads, std_devs


def plot_line_chart_with_std_dev(data, title, xlabel, ylabel):
    x_values = [key for key in range(2, 7)]
    y_values = list(data[0].values())
    std_devs = list(data[1].values())

    # Choose colors for the line and error bars
    line_color = 'skyblue'
    error_bar_color = 'lightcoral'

    # Plot the line with markers
    plt.plot(x_values, y_values, marker='o',
             color=line_color, label='Average Load')

    # Plot error bars for standard deviation
    plt.errorbar(x_values, y_values, yerr=std_devs,
                 linestyle='None', capsize=4, color=error_bar_color)

    # Customize chart details
    plt.title(title, fontsize=16)
    plt.xlabel(xlabel, fontsize=14)
    plt.ylabel(ylabel, fontsize=14)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.tight_layout()

    # Show the plot
    plt.show()


if __name__ == "__main__":
    asyncio.run(generate_requests())
    main_dict = {key: main_dict[key] for key in sorted(main_dict)}
    avg_loads, std_devs = calculate_average_load_and_std_dev()
    avg_loads = {key: avg_loads[key] for key in sorted(avg_loads)}
    std_devs = {key: std_devs[key] for key in sorted(std_devs)}

    plot_line_chart_with_std_dev(
        (avg_loads, std_devs), "Average Load and Standard Deviation", "Number of Servers (N)", "Average Load")
