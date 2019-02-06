import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator

# Hard-coded parameters
min_value = 0
max_value = 10
xmin = 0
xmax = 23
xstep = 1
xlabel = 'X label'
data_label = 'Y label'
chart_name = 'Chart name'

# Hard-coded data
data ={0: 2.3450324675324676, 17: 2.1232577319587627, 21: 2.0996724470134875, 6: 2.094597701149425, 19: 2.0909586776859506, 16: 2.0673258426966292, 5: 2.02041095890411, 12: 1.983922413793103, 8: 1.9621689497716894, 18: 1.9235973597359737, 14: 1.8990376569037661, 22: 1.8856130268199236, 23: 1.8644902912621362, 11: 1.8588815789473683, 2: 1.848867924528302, 20: 1.843728813559322, 15: 1.8333466135458165, 13: 1.8131049250535327, 3: 1.7409615384615384, 9: 1.735978021978022, 10: 1.7179699248120301, 7: 1.6804934210526317, 1: 1.6328415300546446, 4: 1.220689655172414}


majorLocator = MultipleLocator(1)  # Major ticks
minorLocator = MultipleLocator(0.25)  # Minor ticks
fig, ax = plt.subplots()

ax.yaxis.set_major_locator(majorLocator)
ax.yaxis.set_minor_locator(minorLocator)

# Sort the data and get the lists by axis
data = sorted(data.items())
x, y = zip(*data)

# Compute the minimum and maximum Y values
ymin = float(min_value)
ymax = float(max_value) * 1.25

# Range limits
plt.xlim(xmin, xmax)
plt.ylim(ymin, ymax)

# Tick limits and frequency
plt.xticks(np.arange(xmin, xmax+1, xstep))
plt.yticks(np.arange(ymin, ymax, 1))

# Axis labels and chart title
plt.xlabel(xlabel)
plt.ylabel(data_label)
plt.title(chart_name)

plt.scatter(x, y)  # Add the points
plt.plot(x, y)  # Draw the line
plt.grid(axis='x', which='major')  # Add the major X grid
plt.grid(axis='y', which='major')  # Add the major Y grid
plt.grid(axis='y', which='minor', linestyle='--')  # Add the minor Y grid
plt.show()  # Show the chart