import matplotlib.pyplot as plt
import matplotlib.animation as animation
import json


fig = plt.figure(figsize=(8, 6))
# creating a subplot
ax1 = fig.add_subplot(1, 1, 1)
scatter = ax1.scatter([], [])
ann_list = []
ax1.set_ylim(0, 21)
ax1.set_xlim(-0.25, 5)


def animate(i):
    with open("./notebooks/sim_data.json", "r") as f:
        data = json.load(f)
    xs = data["xs"]
    ys = data["ys"]
    texts = data["texts"]

    # human column
    current_human_col_count = 0
    new_xs = []
    new_ys = []
    for x, y in zip(xs, ys):
        if x == 2:
            current_human_col_count += 1
            y = y % 20
            x += 1 * int(current_human_col_count / 21)
        new_xs.append(x)
        new_ys.append(y + 1)

    # ax1.clear()
    # ax1.scatter(xs, ys)
    scatter.set_offsets([(new_xs[ix], new_ys[ix]) for ix in range(len(new_xs))])

    # for _, ann in enumerate(ann_list):
    #     ann.remove()
    # ann_list[:] = []

    # for i, txt in enumerate(texts):
    #     ann_list.append(ax1.annotate(txt, (new_xs[i], new_ys[i]), fontsize=8))

    ax1.plot([1.8, 1.8], [0, 21], linestyle="dashed", color="black")

    plt.xlabel("Position")
    plt.ylabel("Num Tasks")
    plt.title("Pig Latin Translator Progress")
    plt.xticks(
        [0, 1, 2],
        ["agent1", "agent2", "done"],
    )
    plt.yticks(range(21))


ani = animation.FuncAnimation(fig, animate, interval=1000)
plt.show()
