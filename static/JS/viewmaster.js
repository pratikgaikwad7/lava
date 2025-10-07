// Initialize date pickers
flatpickr(".datepicker", {
  dateFormat: "d/m/Y",
  allowInput: true,
});
// Auto-submit on TNI status change
document.getElementById("tni_status").addEventListener("change", function () {
  this.form.submit();
});
// Show loading state on form submit
document.querySelector("form").addEventListener("submit", function () {
  document.querySelector(".table-container").innerHTML = `
        <div class="loading-state">
            <div class="spinner-border" role="status">
                <span class="visually-hidden">Loading...</span>
            </div>
            <p class="mt-3">Applying filters...</p>
        </div>
    `;
});
// Reset pagination on filter change
document.querySelectorAll("form select, form input").forEach((element) => {
  if (element.name !== "page") {
    element.addEventListener("change", function () {
      document.querySelector('input[name="page"]').value = 1;
    });
  }
});
// Applied filters UI update
// Applied filters UI update
function updateAppliedFilters() {
  const form = document.getElementById("filterForm");
  const filtersList = document.getElementById("appliedFiltersList");
  const filtersStrip = document.getElementById("applied-filters");
  const formData = new FormData(form);
  let hasFilters = false;
  filtersList.innerHTML = "";

  formData.forEach((value, key) => {
    if (value && key !== "page" && value !== "All" && value !== "") {
      hasFilters = true;
      const filterItem = document.createElement("span");
      filterItem.className = "applied-filter-item";
      let displayKey = key.replace(/_/g, " ");
      displayKey = displayKey.charAt(0).toUpperCase() + displayKey.slice(1);
      let displayValue = value.replace(/_/g, " ");

      // Special formatting for fiscal year
      if (key === "fiscal_year") {
        const year = parseInt(value);
        if (!isNaN(year)) {
          const nextYear = year + 1;
          const nextYearShort = nextYear.toString().slice(-2); // Get last 2 digits
          displayValue = `FY ${year}-${nextYearShort}`;
          displayKey = "Fiscal Year"; // Keep the key as "Fiscal Year"
        }
      }

      filterItem.innerHTML = `<span class="filter-key">${displayKey}:</span> <span class="filter-value">${displayValue}</span>`;
      filtersList.appendChild(filterItem);
    }
  });

  filtersStrip.style.display = hasFilters ? "block" : "none";
  return hasFilters;
}
// DOM ready
document.addEventListener("DOMContentLoaded", function () {
  const hasQueryParams = window.location.search.includes("=");
  const hasFilters = updateAppliedFilters();
  // Store form submission flag
  document.getElementById("filterForm").addEventListener("submit", function () {
    sessionStorage.setItem("filterSubmitted", "true");
  });
  // After reload, update filters but don't scroll
  if (sessionStorage.getItem("filterSubmitted") === "true") {
    sessionStorage.removeItem("filterSubmitted");
    setTimeout(() => {
      updateAppliedFilters();
    }, 150); // Short delay to ensure layout is ready
  }
});
// Display current date
function displayCurrentDate() {
  const currentDate = new Date();
  const options = { year: "numeric", month: "long", day: "numeric" };
  const formattedDate = currentDate.toLocaleDateString("en-US", options);
  const dateElement = document.getElementById("current-date");
  if (dateElement) {
    dateElement.textContent = "Data updated as of " + formattedDate + ".";
  }
}
displayCurrentDate();
// Update date at midnight
const now = new Date();
const midnight = new Date(
  now.getFullYear(),
  now.getMonth(),
  now.getDate() + 1,
  0,
  0,
  0
);
const timeUntilMidnight = midnight - now;
setTimeout(function () {
  displayCurrentDate();
  setInterval(displayCurrentDate, 86400000);
}, timeUntilMidnight);
// Enhanced table toggle functionality
document.addEventListener("DOMContentLoaded", function () {
  const toggleBtn = document.getElementById("toggleTableBtn");
  const tableContainer = document.getElementById("resultsTable");
  // Check if we have table data to toggle
  if (toggleBtn && tableContainer) {
    // Check if table was previously visible
    const isTableVisible = localStorage.getItem("tableVisible") === "true";
    // Set initial state
    if (isTableVisible) {
      tableContainer.style.display = "block";
      toggleBtn.innerHTML = '<i class="fas fa-eye me-2"></i>Hide Table Data';
      toggleBtn.classList.remove("pulse");
    } else {
      tableContainer.style.display = "none";
      toggleBtn.innerHTML =
        '<i class="fas fa-eye-slash me-2"></i>Show Table Data';
      // Add pulse animation only if there are records
      if (document.querySelector(".table-responsive")) {
        toggleBtn.classList.add("pulse");
      }
    }
    // Toggle button click event
    toggleBtn.addEventListener("click", function () {
      if (tableContainer.style.display === "none") {
        tableContainer.style.display = "block";
        toggleBtn.innerHTML = '<i class="fas fa-eye me-2"></i>Hide Table Data';
        toggleBtn.classList.remove("pulse");
        localStorage.setItem("tableVisible", "true");
        // Add a slight highlight effect to the table
        tableContainer.style.opacity = "0.9";
        setTimeout(() => {
          tableContainer.style.opacity = "1";
        }, 300);
      } else {
        tableContainer.style.display = "none";
        toggleBtn.innerHTML =
          '<i class="fas fa-eye-slash me-2"></i>Show Table Data';
        // Add pulse animation only if there are records
        if (document.querySelector(".table-responsive")) {
          toggleBtn.classList.add("pulse");
        }
        localStorage.setItem("tableVisible", "false");
      }
    });
  }
  // Auto-show table if it contains data and we're on page 3
  const urlParams = new URLSearchParams(window.location.search);
  if (urlParams.toString() && window.location.hash === "#page3") {
    setTimeout(() => {
      if (tableContainer && document.querySelector(".table-responsive")) {
        tableContainer.style.display = "block";
        if (toggleBtn) {
          toggleBtn.innerHTML =
            '<i class="fas fa-eye me-2"></i>Hide Table Data';
          toggleBtn.classList.remove("pulse");
        }
        localStorage.setItem("tableVisible", "true");
      }
    }, 500);
  }
  // Add keyboard shortcut (Alt+T) to toggle table
  document.addEventListener("keydown", function (e) {
    if (e.altKey && e.key.toLowerCase() === "t") {
      e.preventDefault();
      if (toggleBtn) {
        toggleBtn.click();
      }
    }
  });
});
document.addEventListener("DOMContentLoaded", function () {
  const filterStrip = document.getElementById("applied-filters");

  // Update the current date
  function updateDate() {
    const now = new Date();
    const options = { year: "numeric", month: "short", day: "numeric" };
    document.getElementById(
      "current-date"
    ).textContent = now.toLocaleDateString(undefined, options);
  }
  updateDate();
  // Update date every minute
  setInterval(updateDate, 60000);
});
document.addEventListener("DOMContentLoaded", function () {
  const filterToggleBtn = document.getElementById("filterToggleBtn");
  const filterSidePanel = document.getElementById("filterSidePanel");
  const filterControlArea = document.getElementById("filterControlArea");
  const filterLabelContainer = document.getElementById("filterLabelContainer"); // Add this line
  // Toggle filter panel when clicking the toggle button
  filterToggleBtn.addEventListener("click", function () {
    filterSidePanel.classList.toggle("active");
    filterControlArea.classList.toggle("panel-open");
  });
  // Toggle filter panel when clicking the "Choose Filter" label
  filterLabelContainer.addEventListener("click", function () {
    filterSidePanel.classList.toggle("active");
    filterControlArea.classList.toggle("panel-open");
  });
  // Close filter panel when clicking outside
  document.addEventListener("click", function (event) {
    if (
      !filterSidePanel.contains(event.target) &&
      !filterToggleBtn.contains(event.target) &&
      !filterControlArea.contains(event.target) &&
      filterSidePanel.classList.contains("active")
    ) {
      filterSidePanel.classList.remove("active");
      filterControlArea.classList.remove("panel-open");
    }
  });
  // Hide filter panel when filters are applied
  const filterForm = document.getElementById("filterForm");
  if (filterForm) {
    filterForm.addEventListener("submit", function () {
      filterSidePanel.classList.remove("active");
      filterControlArea.classList.remove("panel-open");
    });
  }
  // Also hide when reset button is clicked
  const resetButton = document.querySelector(
    "a[href=\"{{ url_for('view_bp.view_master_data') }}\"]"
  );
  if (resetButton) {
    resetButton.addEventListener("click", function () {
      filterSidePanel.classList.remove("active");
      filterControlArea.classList.remove("panel-open");
    });
  }
  // And when export button is clicked
  const exportButton = document.querySelector('a[href*="download_excel"]');
  if (exportButton) {
    exportButton.addEventListener("click", function () {
      filterSidePanel.classList.remove("active");
      filterControlArea.classList.remove("panel-open");
    });
  }
});

// Chart selection and download functionality
document.addEventListener("DOMContentLoaded", function () {
  initializeChartSelection();
});

function initializeChartSelection() {
  const availableCharts = [
    {
      id: "chartEOR-container",
      title: "EOR Count by Male/Female",
      icon: "fa-chart-pie",
    },
    {
      id: "employeeGroupChart-container",
      title: "Employee Distribution by Group",
      icon: "fa-chart-bar",
    },
    {
      id: "ulEorChart-container",
      title: "Permanent technician vs Unique Learners",
      icon: "fa-chart-line",
    },
    {
      id: "annualYtdChart-container",
      title: "TNI YTD (Year Till Date) Status",
      icon: "fa-chart-area",
    },
    {
      id: "16HourChart-container",
      title: "16+ Learning hours compliance YTD Status",
      icon: "fa-chart-pie",
    },
    {
      id: "sheStackedChart-container",
      title: "6+ Safety Learning hours compliance YTD Status",
      icon: "fa-chart-bar",
    },
    { id: "pl1Chart-container", title: "PL1 YTD Status", icon: "fa-chart-pie" },
    { id: "pl2Chart-container", title: "PL2 YTD Status", icon: "fa-chart-pie" },
    { id: "pl3Chart-container", title: "PL3 YTD Status", icon: "fa-chart-pie" },
    {
      id: "monthwiseYTDChart-container",
      title: "TNI Month-wise YTD Plan vs Actual Coverage vs Adherence",
      icon: "fa-chart-line",
    },
    {
      id: "sheCategoryChart-container",
      title: "Annual Safety Coverage Status",
      icon: "fa-chart-bar",
    },
    {
      id: "pmoCategoryChart-container",
      title: "Annual PMO Category Coverage Status",
      icon: "fa-chart-bar",
    },
    {
      id: "chartCESS-container",
      title: "Annual CESS Training Coverage Status",
      icon: "fa-chart-bar",
    },
    {
      id: "chartDigital-container",
      title: "Annual Digital Training Coverage Status",
      icon: "fa-chart-bar",
    },
    {
      id: "chartFS-container",
      title: "Annual Functional Skills Coverage Status",
      icon: "fa-chart-bar",
    },
    {
      id: "chartPS-container",
      title: "Annual Professional Skills Coverage Status",
      icon: "fa-chart-bar",
    },
    {
      id: "chartSHE-container",
      title: "Annual Safety Training Coverage Status",
      icon: "fa-chart-bar",
    },
    {
      id: "chartSust-container",
      title: "Annual Sustainability Coverage Status",
      icon: "fa-chart-bar",
    },
  ];

  const chartSelectionList = document.getElementById("chartSelectionList");

  if (chartSelectionList) {
    availableCharts.forEach((chart) => {
      const chartItem = document.createElement("div");
      chartItem.className = "chart-selection-item";
      chartItem.innerHTML = `
        <input type="checkbox" id="chart-${chart.id}" value="${chart.id}">
        <div class="chart-selection-icon"><i class="fas ${chart.icon}"></i></div>
        <div class="chart-selection-title">${chart.title}</div>
      `;
      chartSelectionList.appendChild(chartItem);

      const checkbox = chartItem.querySelector('input[type="checkbox"]');

      // ✅ Prevent dropdown from closing when clicking inside chart item
      chartItem.addEventListener("click", function (e) {
        e.stopPropagation(); // Important to keep dropdown open
        if (e.target.tagName !== "INPUT") {
          checkbox.checked = !checkbox.checked;
          checkbox.dispatchEvent(new Event("change"));
        }
      });

      // Prevent dropdown from closing when clicking checkbox directly
      checkbox.addEventListener("click", function (e) {
        e.stopPropagation();
      });

      // Update download button state
      checkbox.addEventListener("change", updateDownloadButtonState);
    });
  }

  // Select all functionality
  const selectAllCharts = document.getElementById("selectAllCharts");
  if (selectAllCharts) {
    selectAllCharts.addEventListener("click", function (e) {
      e.stopPropagation(); // prevent dropdown close
      const checkboxes = chartSelectionList.querySelectorAll(
        'input[type="checkbox"]'
      );
      const allChecked = Array.from(checkboxes).every((cb) => cb.checked);
      checkboxes.forEach((checkbox) => {
        checkbox.checked = !allChecked;
        checkbox.dispatchEvent(new Event("change"));
      });
      updateDownloadButtonState();
    });
  }

  // ✅ FIXED: Download selected charts with proper data label handling
  const downloadSelectedCharts = document.getElementById(
    "downloadSelectedCharts"
  );
  if (downloadSelectedCharts) {
    downloadSelectedCharts.addEventListener("click", async function (e) {
      e.stopPropagation(); // prevent dropdown close
      const selectedCharts = [];
      chartSelectionList
        .querySelectorAll('input[type="checkbox"]:checked')
        .forEach((cb) => selectedCharts.push(cb.value));

      if (selectedCharts.length > 0) {
        try {
          // Show loading state
          downloadSelectedCharts.innerHTML =
            '<i class="fas fa-spinner fa-spin"></i> Downloading...';
          downloadSelectedCharts.disabled = true;

          for (const chartId of selectedCharts) {
            await downloadChartAsEnlarged(chartId);

            // Small delay between downloads to prevent browser overload
            await new Promise((resolve) => setTimeout(resolve, 1000));
          }

          // Reset button state
          downloadSelectedCharts.innerHTML =
            '<i class="fas fa-download"></i> Download Selected Charts';
          downloadSelectedCharts.disabled = false;
        } catch (error) {
          console.error("Error downloading charts:", error);
          alert("Error downloading charts. Please try again.");

          // Reset button state on error
          downloadSelectedCharts.innerHTML =
            '<i class="fas fa-download"></i> Download Selected Charts';
          downloadSelectedCharts.disabled = false;
        }
      }
    });
  }

  function updateDownloadButtonState() {
    const checkedBoxes = chartSelectionList.querySelectorAll(
      'input[type="checkbox"]:checked'
    );
    const downloadButton = document.getElementById("downloadSelectedCharts");
    if (downloadButton) downloadButton.disabled = checkedBoxes.length === 0;
  }
}

// ✅ FIXED: Enhanced download function with proper SHE chart handling
async function downloadChartAsEnlarged(chartId) {
  return new Promise(async (resolve, reject) => {
    try {
      const chartContainer = document.getElementById(chartId);
      if (!chartContainer) {
        reject(new Error(`Chart container not found: ${chartId}`));
        return;
      }

      const originalCanvas = chartContainer.querySelector("canvas");
      if (!originalCanvas) {
        reject(new Error(`Canvas not found in chart: ${chartId}`));
        return;
      }

      const originalChart = Chart.getChart(originalCanvas);
      if (!originalChart) {
        reject(new Error(`Chart instance not found: ${chartId}`));
        return;
      }

      console.log(`Downloading chart: ${chartId}`, originalChart.config);

      // ✅ DEBUG: Log specific information for SHE chart
      if (chartId.includes("she") || chartId.includes("SHE")) {
        console.log("SHE Chart detected - Config:", {
          type: originalChart.config.type,
          indexAxis: originalChart.config.options?.indexAxis,
          datasets: originalChart.config.data?.datasets?.map((d) => ({
            label: d.label,
            data: d.data,
            type: d.type,
          })),
        });
      }

      // ✅ Use enhanced cloning function
      const enlargedConfig = safeCloneChartConfig(originalChart);

      // Create temporary container for high-quality rendering
      const tempContainer = document.createElement("div");
      tempContainer.style.position = "fixed";
      tempContainer.style.left = "-9999px";
      tempContainer.style.top = "0";
      tempContainer.style.width = "1200px";
      tempContainer.style.height = "800px";
      tempContainer.style.backgroundColor = "#ffffff";
      tempContainer.style.padding = "40px";
      tempContainer.style.boxSizing = "border-box";
      tempContainer.style.zIndex = "10000";
      document.body.appendChild(tempContainer);

      // Create canvas for high-quality download
      const downloadCanvas = document.createElement("canvas");
      downloadCanvas.width = 1200;
      downloadCanvas.height = 800;
      downloadCanvas.style.width = "1120px"; // Account for padding
      downloadCanvas.style.height = "720px";
      downloadCanvas.style.backgroundColor = "#ffffff";

      tempContainer.appendChild(downloadCanvas);

      // Create the chart with enhanced configuration
      const downloadCtx = downloadCanvas.getContext("2d");

      // Wait for container to render
      await new Promise((resolve) => setTimeout(resolve, 100));

      const downloadChart = new Chart(downloadCtx, enlargedConfig);

      // ✅ FIXED: Longer wait time specifically for SHE charts
      const isSHEChart = chartId.includes("she") || chartId.includes("SHE");
      const isHorizontalBar = enlargedConfig.options?.indexAxis === "y";

      // Longer wait time for complex charts with data labels
      const waitTime = isSHEChart ? 2500 : isHorizontalBar ? 2000 : 1500;
      console.log(`Waiting ${waitTime}ms for ${chartId} to render...`);
      await new Promise((resolve) => setTimeout(resolve, waitTime));

      // Create final canvas for download
      const finalCanvas = document.createElement("canvas");
      finalCanvas.width = 1200;
      finalCanvas.height = 800;
      const finalCtx = finalCanvas.getContext("2d");

      // Set white background
      finalCtx.fillStyle = "#ffffff";
      finalCtx.fillRect(0, 0, finalCanvas.width, finalCanvas.height);

      // Draw the chart onto final canvas
      finalCtx.drawImage(
        downloadCanvas,
        0,
        0,
        finalCanvas.width,
        finalCanvas.height
      );

      // Convert to image and download
      const imgData = finalCanvas.toDataURL("image/png", 1.0);
      const link = document.createElement("a");
      link.href = imgData;
      link.download = `${getChartTitle(chartId)}_${new Date().getTime()}.png`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);

      // Clean up
      downloadChart.destroy();
      document.body.removeChild(tempContainer);

      resolve();
    } catch (error) {
      console.error("Error downloading chart:", error);

      // ✅ FALLBACK: Try html2canvas if available for SHE charts
      if (
        (chartId.includes("she") || chartId.includes("SHE")) &&
        typeof html2canvas !== "undefined"
      ) {
        console.log("Trying html2canvas fallback for SHE chart");
        try {
          await downloadChartScreenshot(chartId);
          resolve();
          return;
        } catch (fallbackError) {
          console.error("html2canvas fallback also failed:", fallbackError);
        }
      }

      reject(error);
    }
  });
}

// ✅ FIXED: Enhanced safe chart configuration cloning with proper SHE chart handling
function safeCloneChartConfig(originalChart) {
  const originalConfig = originalChart.config;

  // Create a clean configuration object
  const config = {
    type: originalConfig.type,
    data: {
      labels: [...(originalConfig.data.labels || [])],
      datasets: [],
    },
    options: {
      responsive: false,
      maintainAspectRatio: false,
      animation: {
        duration: 0, // Disable animations for consistent exports
      },
      plugins: {},
    },
  };

  // ✅ FIXED: Enhanced dataset cloning with proper indexAxis handling for SHE chart
  if (originalConfig.data && originalConfig.data.datasets) {
    originalConfig.data.datasets.forEach((originalDataset, index) => {
      const dataset = {
        label: originalDataset.label || `Dataset ${index + 1}`,
        data: [...(originalDataset.data || [])],
        type: originalDataset.type, // Preserve type (bar/line)
        yAxisID: originalDataset.yAxisID, // ✅ CRITICAL: Preserve yAxisID
        order: originalDataset.order, // ✅ CRITICAL: Preserve dataset order
        hidden: originalDataset.hidden,
      };

      // ✅ CRITICAL FIX: Preserve indexAxis for horizontal bar charts (like SHE chart)
      if (originalConfig.options && originalConfig.options.indexAxis) {
        config.options.indexAxis = originalConfig.options.indexAxis;
      }

      // Handle background colors - ensure arrays are properly cloned
      if (Array.isArray(originalDataset.backgroundColor)) {
        dataset.backgroundColor = [...originalDataset.backgroundColor];
      } else if (originalDataset.backgroundColor) {
        dataset.backgroundColor = originalDataset.backgroundColor;
      } else {
        // Provide fallback colors
        const fallbackColors = [
          "rgba(54, 162, 235, 0.8)",
          "rgba(255, 99, 132, 0.8)",
          "rgba(75, 192, 192, 0.8)",
          "rgba(255, 159, 64, 0.8)",
          "rgba(153, 102, 255, 0.8)",
        ];
        dataset.backgroundColor = fallbackColors[index % fallbackColors.length];
      }

      // Handle border colors
      if (Array.isArray(originalDataset.borderColor)) {
        dataset.borderColor = [...originalDataset.borderColor];
      } else if (originalDataset.borderColor) {
        dataset.borderColor = originalDataset.borderColor;
      } else {
        dataset.borderColor = dataset.backgroundColor;
      }

      // Copy other critical properties
      const propertiesToCopy = [
        "borderWidth",
        "fill",
        "tension",
        "radius",
        "cutout",
        "pointRadius",
        "pointBackgroundColor",
        "pointBorderColor",
        "pointBorderWidth",
        "pointHoverRadius",
        "barPercentage",
        "categoryPercentage",
        "barThickness",
        "maxBarThickness",
        "indexAxis", // ✅ ADDED: Preserve indexAxis at dataset level if exists
      ];

      propertiesToCopy.forEach((prop) => {
        if (originalDataset[prop] !== undefined) {
          dataset[prop] = originalDataset[prop];
        }
      });

      config.data.datasets.push(dataset);
    });
  }

  // ✅ FIXED: Enhanced options cloning with proper scales configuration for SHE chart
  if (originalConfig.options) {
    // Clone plugins
    if (originalConfig.options.plugins) {
      config.options.plugins = { ...originalConfig.options.plugins };

      // Clone title with enhanced size
      if (originalConfig.options.plugins.title) {
        config.options.plugins.title = {
          ...originalConfig.options.plugins.title,
          display: true,
          font: {
            ...(originalConfig.options.plugins.title.font || {}),
            size: 24,
            weight: "bold",
          },
        };
      }

      // Clone legend with enhanced settings
      if (originalConfig.options.plugins.legend) {
        config.options.plugins.legend = {
          ...originalConfig.options.plugins.legend,
          display: true,
          position: originalConfig.options.plugins.legend.position || "top",
          labels: {
            ...originalConfig.options.plugins.legend.labels,
            font: {
              size: 18,
              weight: "bold",
            },
            padding: 20,
            usePointStyle: true,
            boxWidth: 14,
          },
        };
      }

      // ✅ CRITICAL FIX: Enhanced datalabels scaling for exports
      if (originalConfig.options.plugins.datalabels) {
        config.options.plugins.datalabels = {
          ...originalConfig.options.plugins.datalabels,
          font: {
            ...(originalConfig.options.plugins.datalabels.font || {}),
            size: 18,
            weight: "bold",
          },
          align: originalConfig.options.plugins.datalabels.align || "center",
          anchor: originalConfig.options.plugins.datalabels.anchor || "center",
          formatter: originalConfig.options.plugins.datalabels.formatter,
          padding: 6,
        };
      }
    }

    // ✅ CRITICAL FIX: Enhanced scales cloning for horizontal bar charts
    if (originalConfig.options.scales) {
      config.options.scales = {};

      Object.keys(originalConfig.options.scales).forEach((scaleId) => {
        const originalScale = originalConfig.options.scales[scaleId];
        if (originalScale) {
          config.options.scales[scaleId] = { ...originalScale };

          // Ensure scale is visible in exports
          config.options.scales[scaleId].display = true;

          // Enhance ticks with larger font
          if (originalScale.ticks) {
            config.options.scales[scaleId].ticks = {
              ...originalScale.ticks,
              font: {
                ...(originalScale.ticks.font || {}),
                size: 14,
                weight: "bold",
              },
              callback: originalScale.ticks.callback,
            };
          }

          // Enhance title with larger font
          if (originalScale.title) {
            config.options.scales[scaleId].title = {
              ...originalScale.title,
              display: true,
              font: {
                ...(originalScale.title.font || {}),
                size: 16,
                weight: "bold",
              },
            };
          }

          // Ensure grid lines are preserved
          if (originalScale.grid) {
            config.options.scales[scaleId].grid = {
              ...originalScale.grid,
            };
          }
        }
      });
    }

    // ✅ CRITICAL FIX: Preserve indexAxis for horizontal bar charts (SHE chart)
    if (originalConfig.options.indexAxis) {
      config.options.indexAxis = originalConfig.options.indexAxis;
    }

    // Copy layout if it exists
    if (originalConfig.options.layout) {
      config.options.layout = { ...originalConfig.options.layout };

      // Ensure proper padding for exports
      if (!config.options.layout.padding) {
        config.options.layout.padding = {
          top: 20,
          right: 20,
          bottom: 20,
          left: 20,
        };
      }
    }
  }

  // ✅ FIXED: Enhanced plugin preservation for SHE chart
  if (originalConfig.plugins && Array.isArray(originalConfig.plugins)) {
    config.plugins = [...originalConfig.plugins];

    // Enhance any custom plugins that handle data labels
    config.plugins = config.plugins.map((plugin) => {
      if (plugin && plugin.id === "valueLabelsPlugin") {
        return {
          ...plugin,
          afterDatasetsDraw: function (chart) {
            const { ctx, data, chartArea } = chart;
            const topClamp = chartArea.top + 25;
            ctx.save();
            data.datasets.forEach((ds, dsIndex) => {
              const meta = chart.getDatasetMeta(dsIndex);
              meta.data.forEach((point, i) => {
                let val = ds.data[i];
                if (val == null || isNaN(val)) return;

                // ✅ Only add % for Adherence dataset
                if (ds.label && ds.label.includes("%")) {
                  val = val + "%";
                }

                const pos = point.tooltipPosition
                  ? point.tooltipPosition()
                  : { x: point.x, y: point.y };
                let x, y, color;

                // ✅ FIXED: Better handling for horizontal bar charts (SHE chart)
                if (chart.options.indexAxis === "y") {
                  // Horizontal bars - position labels differently
                  const xZero = chart.scales.x.getPixelForValue(0);
                  const barWidth = Math.abs(point.x - xZero);
                  x = point.x + (point.x < xZero ? -12 : 12);
                  if (barWidth < 30) x = point.x - 12;
                  color = "#000";

                  // Draw text for horizontal bars
                  ctx.font = "bold 16px Segoe UI, Arial, sans-serif";
                  ctx.textAlign = point.x < xZero ? "right" : "left";
                  ctx.textBaseline = "middle";
                  ctx.fillStyle = "#000000";
                  ctx.fillText(val, x, point.y);
                } else {
                  // Vertical bars
                  if (ds.type === "bar") {
                    const yZero = chart.scales.y.getPixelForValue(0);
                    const barHeight = Math.abs(point.y - yZero);
                    y = point.y + (point.y < yZero ? -12 : 12);
                    if (barHeight < 30) y = point.y - 12;
                    color = "#000";
                  } else {
                    // Line charts
                    y = point.y - 20;
                    color = ds.borderColor || "#000";

                    // Special handling for line charts to avoid overlap
                    if (dsIndex > 0) {
                      const prevMeta = chart.getDatasetMeta(dsIndex - 1);
                      if (prevMeta && prevMeta.data[i]) {
                        const prevPoint = prevMeta.data[i];
                        if (Math.abs(point.y - prevPoint.y) < 25) {
                          y = point.y - 35;
                        }
                      }
                    }
                  }

                  if (y < topClamp) y = topClamp;

                  // Enhanced font styling
                  ctx.font = "bold 16px Segoe UI, Arial, sans-serif";
                  ctx.textAlign = "center";
                  ctx.textBaseline = "middle";
                  ctx.fillStyle = "#000000";
                  ctx.fillText(val, pos.x, y);
                }
              });
            });
            ctx.restore();
          },
        };
      }

      // Handle ChartDataLabels plugin specifically
      if (
        plugin &&
        typeof plugin === "function" &&
        plugin.id === "datalabels"
      ) {
        // Ensure ChartDataLabels plugin is properly configured
        return {
          ...plugin,
          defaults: {
            ...(plugin.defaults || {}),
            font: {
              ...((plugin.defaults && plugin.defaults.font) || {}),
              size: 18,
              weight: "bold",
            },
          },
        };
      }

      return plugin;
    });
  }

  console.log("Enhanced cloned config for SHE chart:", config);
  return config;
}

// ✅ ENHANCED: Helper function to ensure proper bar+line chart configuration
function createComboChartConfig(baseConfig) {
  // Ensure datasets have proper order and yAxisID for combo charts
  const hasLine = baseConfig.data.datasets.some((ds) => ds.type === "line");
  const hasBar = baseConfig.data.datasets.some(
    (ds) => !ds.type || ds.type === "bar"
  );

  if (hasLine && hasBar) {
    // Set proper order: lines on top of bars
    baseConfig.data.datasets.forEach((dataset, index) => {
      if (dataset.type === "line") {
        dataset.order = 1; // Lines render on top
      } else {
        dataset.order = 2; // Bars render below
      }

      // Ensure yAxisID is properly set
      if (
        dataset.type === "line" &&
        !dataset.yAxisID &&
        baseConfig.options.scales?.percentageAxis
      ) {
        dataset.yAxisID = "percentageAxis";
      }

      // ✅ ENHANCED: Ensure line chart data points are visible
      if (dataset.type === "line") {
        dataset.pointRadius = 6; // Increased from default
        dataset.pointHoverRadius = 8;
        dataset.pointBorderWidth = 2;
      }
    });

    // Ensure scales are properly configured
    if (!baseConfig.options.scales) {
      baseConfig.options.scales = {};
    }

    if (
      !baseConfig.options.scales.y &&
      !baseConfig.options.scales.percentageAxis
    ) {
      baseConfig.options.scales = {
        y: {
          beginAtZero: true,
          title: {
            display: true,
            text: "Employee Counts",
            font: {
              size: 16,
              weight: "bold",
            },
          },
          ticks: {
            font: {
              size: 14,
            },
          },
        },
        percentageAxis: {
          position: "right",
          beginAtZero: true,
          ticks: {
            callback: (v) => v + "%",
            font: {
              size: 14,
            },
          },
          title: {
            display: true,
            text: "Adherence %",
            font: {
              size: 16,
              weight: "bold",
            },
          },
          grid: { drawOnChartArea: false },
        },
      };
    }
  }

  return baseConfig;
}

// ✅ ALTERNATIVE: Simple screenshot approach using html2canvas
async function downloadChartScreenshot(chartId) {
  return new Promise((resolve, reject) => {
    try {
      const chartContainer = document.getElementById(chartId);
      if (!chartContainer) {
        reject(new Error(`Chart container not found: ${chartId}`));
        return;
      }

      // Check if html2canvas is available
      if (typeof html2canvas === "undefined") {
        reject(
          new Error(
            'html2canvas library not loaded. Please add: <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>'
          )
        );
        return;
      }

      // Create a temporary container to ensure proper rendering
      const tempContainer = document.createElement("div");
      tempContainer.style.position = "fixed";
      tempContainer.style.left = "0";
      tempContainer.style.top = "0";
      tempContainer.style.width = "1200px";
      tempContainer.style.height = "800px";
      tempContainer.style.backgroundColor = "#ffffff";
      tempContainer.style.padding = "20px";
      tempContainer.style.zIndex = "10000";
      tempContainer.style.boxSizing = "border-box";

      // Clone the chart container
      const clonedChart = chartContainer.cloneNode(true);
      clonedChart.style.width = "100%";
      clonedChart.style.height = "100%";

      tempContainer.appendChild(clonedChart);
      document.body.appendChild(tempContainer);

      // Use html2canvas to capture the chart
      html2canvas(tempContainer, {
        backgroundColor: "#ffffff",
        scale: 2,
        logging: false,
        useCORS: true,
        width: 1200,
        height: 800,
      })
        .then((canvas) => {
          const link = document.createElement("a");
          link.href = canvas.toDataURL("image/png", 1.0);
          link.download = `${getChartTitle(
            chartId
          )}_${new Date().getTime()}.png`;
          document.body.appendChild(link);
          link.click();
          document.body.removeChild(link);

          // Clean up
          document.body.removeChild(tempContainer);
          resolve();
        })
        .catch((error) => {
          document.body.removeChild(tempContainer);
          reject(error);
        });
    } catch (error) {
      reject(error);
    }
  });
}

// ✅ FIXED: Download currently enlarged chart from modal
function downloadEnlargedChart() {
  const enlargedCanvas = document.getElementById("enlargedChart");
  if (!enlargedCanvas) {
    alert("Please open a chart in enlarged view first");
    return;
  }

  const enlargedChart = Chart.getChart(enlargedCanvas);
  if (!enlargedChart) {
    alert("No chart found in enlarged view");
    return;
  }

  try {
    // Create a high-resolution canvas
    const tempCanvas = document.createElement("canvas");
    tempCanvas.width = enlargedCanvas.width * 2;
    tempCanvas.height = enlargedCanvas.height * 2;

    const tempCtx = tempCanvas.getContext("2d");

    // Scale for high DPI and set white background
    tempCtx.fillStyle = "#ffffff";
    tempCtx.fillRect(0, 0, tempCanvas.width, tempCanvas.height);
    tempCtx.scale(2, 2);

    // Draw the enlarged chart
    tempCtx.drawImage(enlargedCanvas, 0, 0);

    // Download
    const link = document.createElement("a");
    link.download = `chart_${new Date().getTime()}.png`;
    link.href = tempCanvas.toDataURL("image/png", 1.0);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  } catch (error) {
    console.error("Error downloading enlarged chart:", error);
    alert("Error downloading chart. Please try again.");
  }
}

// ✅ Add download button to modal
function addDownloadButtonToModal() {
  const modalFooter = document.querySelector("#chartModal .modal-footer");
  if (modalFooter && !modalFooter.querySelector(".download-enlarged-btn")) {
    const downloadBtn = document.createElement("button");
    downloadBtn.className = "btn btn-success download-enlarged-btn";
    downloadBtn.innerHTML =
      '<i class="fas fa-download"></i> Download This Chart';
    downloadBtn.onclick = downloadEnlargedChart;
    modalFooter.appendChild(downloadBtn);
  }
}

// Initialize modal download button when modal is shown
document.addEventListener("DOMContentLoaded", function () {
  const chartModal = document.getElementById("chartModal");
  if (chartModal) {
    chartModal.addEventListener("shown.bs.modal", function () {
      addDownloadButtonToModal();
    });
  }
});

// Load script helper
function loadScript(src) {
  return new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = src;
    script.onload = resolve;
    script.onerror = reject;
    document.head.appendChild(script);
  });
}

function getChartTitle(chartId) {
  const chartTitles = {
    "chartEOR-container": "EOR Count by Male/Female",
    "employeeGroupChart-container": "Employee Distribution by Group",
    "ulEorChart-container": "Permanent technician vs Unique Learners",
    "annualYtdChart-container": "TNI YTD (Year Till Date) Status",
    "16HourChart-container": "16+ Learning hours compliance YTD Status",
    "sheStackedChart-container":
      "6+ Safety Learning hours compliance YTD Status",
    "pl1Chart-container": "PL1 YTD Status",
    "pl2Chart-container": "PL2 YTD Status",
    "pl3Chart-container": "PL3 YTD Status",
    "monthwiseYTDChart-container":
      "TNI Month-wise YTD Plan vs Actual Coverage vs Adherence",
    "sheCategoryChart-container": "Annual Safety Coverage Status",
    "pmoCategoryChart-container": "Annual PMO Category Coverage Status",
    "chartCESS-container": "Annual CESS Training Coverage Status",
    "chartDigital-container": "Annual Digital Training Coverage Status",
    "chartFS-container": "Annual Functional Skills Coverage Status",
    "chartPS-container": "Annual Professional Skills Coverage Status",
    "chartSHE-container": "Annual Safety Training Coverage Status",
    "chartSust-container": "Annual Sustainability Coverage Status",
  };
  return chartTitles[chartId] || "Chart";
}
