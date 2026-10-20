# Social Media Insights — User Guide

---

## 1. Overview
**Social Media Insights** is an intelligence dashboard to search, monitor, and summarize data across **14 social media platforms**: Twitter/X, Telegram, Instagram, Reddit, Facebook, YouTube, LinkedIn, WhatsApp, Pinterest, Snapchat, Mastodon, Truth Social, TikTok, and Baidu.

Access the dashboard via web browser at:
```text
http://<SERVER_IP>:8085/
```

The application has 3 main tabs:
1. **Report Generator**
2. **Daily Digest**
3. **Alert Sessions**

---

## 2. Tab 1: Report Generator

Use this tab to generate on-demand intelligence reports for any topic or account.

* **Step 1: Enter Keywords & Handles**  
  In the search bar, enter the topics or accounts you want to search. Prefix accounts with `@` (e.g., `@anshu217`, `@mfa_china`) and enter plain keywords directly (e.g., `nepal floods`, `border security`). Separate multiple items with commas.

* **Step 2: Select Platforms**  
  Under **PLATFORMS**, check the platforms from which you need the data. Use the **all** link to select all platforms or **none** to clear selections.

* **Step 3: Set Date Range**  
  Select the **START DATE** and **END DATE** for the period of data needed.  
  *(Note: Dates default to today. Future dates cannot be selected, and Start Date cannot be after End Date).*

* **Step 4: Select Depth & Click 'Generate Report'**  
  - Choose **Summary** for a quick situational overview, or **Report** for an in-depth operational breakdown.
  - Click the **Generate Report** button to fetch the posts and display the AI summary along with the numbered source cards.

* **Step 5: Send via Email**  
  - **For registered users**: Choose an analyst from the **Analyst** dropdown list.
  - **For non-registered emails**: Click **+ Custom email** and type one or more email addresses separated by commas.
  - Click **✉ Send** to email the complete report and sources list.

* **Step 6: Download PDF Locally**  
  Click **⬇ Download PDF** at the top right of the report to download and save a clean PDF of the report locally on your machine (without any browser headers or URLs).

---

## 3. Tab 2: Daily Digest

Use this tab to schedule an automated daily briefing delivered to email every morning.

* **Keywords & Handles**: Enter the topics and accounts you want to track daily.
* **Platforms**: Check the required platforms from the 14-platform checklist on the right.
* **Delivery Time & Timezone**: Set the daily send time in 24-hour format (e.g., `08:00`) and timezone (`Asia/Kolkata`).
* **Recipient**:
  - Choose an analyst from the dropdown for registered users.
  - Or click **+ Custom email** to enter custom email addresses for non-registered users.
* **Enabled**: Keep the checkbox checked to ensure the schedule runs automatically every day.
* **Save Settings**: Click **Save Settings** to save and activate the daily schedule.
* **Send Test Now**: Click **Send Test Now** to immediately trigger a test digest and verify email delivery.
* **Run History**: View the table at the bottom to check past sent digests, timestamps, post counts, and delivery statuses with built-in 5-row pagination (using **Previous** and **Next** buttons).

---

## 4. Tab 3: Alert Sessions

Use this tab for continuous monitoring sessions that scan incoming data and alert you at set intervals.

### Creating an Alert Session:
Click **+ New Alert Session** and configure the options:

* **1. Session Name**: Enter a name for the alert session (e.g., `Border Surveillance`).
* **2. Select Handles & Platforms**: Browse handles loaded directly from the database across the platforms. Check the handles you want to monitor, or use **Select all / Clear all** for each platform tab.
* **3. Keywords & Handles (Optional)**: Type any extra keywords or `@handles` you want to apply across platforms.
* **4. Initial Lookback Date Range**: Choose how far back in the database to fetch posts for the first alert report:
  - Preset options: **Last 24 Hours**, **Last 48 Hours**, or **Last 7 Days**.
  - **Custom Range**: Enter a custom **START DATE** and **END DATE** (future dates are blocked; Start Date cannot be after End Date).
* **5. Recurring Alert Frequency**:
  - Choose a preset frequency: `1x/day (24h)`, `2x/day (12h)`, `3x/day (8h)`, or `4x/day (6h)`.
  - Or select **Custom** to enter specific daily times (e.g., `00:00, 12:00`).
* **6. Send Alert Report To**:
  - Select an analyst from the dropdown for registered users.
  - Or click **+ Custom email** to enter non-registered email addresses.
* **7. Status & Save**: Set status to **Active** (or Paused) and click **Save Session**.

### Using the Session Cards:
Each session card on the dashboard gives you the following options:
* **Run Now**: Immediately execute an alert check for the current window.
* **Pause / Resume**: Temporarily stop or restart background monitoring.
* **Edit**: Modify keywords, handles, schedules, or recipients.
* **History**: View past alert run logs, post counts, and delivery details.
* **Delete**: Permanently remove the session.
