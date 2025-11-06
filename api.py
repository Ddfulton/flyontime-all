from flask import Flask
from flask_cors import CORS
import polars as pl
import pickle

with open('chrome.pkl', 'rb') as f:
    chrome = pickle.load(f)

app = Flask(__name__)
CORS(app)
# df = pl.read_csv(data.csv)

@app.route('/<dayOfWeek>/<monthOfYear>/<origin>/<waypoint>/<airline>/<depHour>/<layover>')
def main(dayOfWeek, monthOfYear, origin, waypoint, airline, depHour, layover):
    cols = [
        'pGreaterThan60',
        'pLessThan15',
        'delayMean',
        'delay90th',
        'pCancel',
        'delayStd',
        'n',
        'shape',
        'scale'
    ]
    res5 = chrome[5].filter(
        pl.col('DayOfWeek') == int(dayOfWeek)
    ).filter(
        pl.col('Month') == int(monthOfYear)
    ).filter(
        pl.col('Origin') == origin
    ).filter(
        pl.col('Airline') == airline
    ).filter(
        pl.col('Hour') == int(depHour)
    )

    if res5[0,0] is None:
        res4 = chrome[4].filter(
            pl.col('Month') == int(monthOfYear)
        ).filter(
            pl.col('Origin') == origin
        ).filter(
            pl.col('Airline') == airline
        ).filter(
            pl.col('Hour') == int(depHour)
        )
        if res4[0,0] is None:
            res3 = chrome[3].filter(
                pl.col('Month') == int(monthOfYear)
            ).filter(
                pl.col('Airline') == airline
            ).filter(
                pl.col('Hour') == int(depHour)
            )
            if res3[0,0] is None:
                return "No matches :("
            else:
                res = res3
                detail = "3"
        else:
            res = res4
            detail = "4"
    else:
        res = res5
        detail = "5"

    row = res.select(cols).mean()

    if row['pLessThan15'][0] is None:
        return 'Not enough data for this flight'
    onTime = round(100*row['pLessThan15'][0])  # % of flights with less than 15 minute delay
    tooLate = round(100*row['pGreaterThan60'][0])  # % of flights with greater than 60 minute delay
    delayMean = round(row['delayMean'][0])
    delayStd = round(row['delayStd'][0])

    shape = row['shape'][0]
    scale = row['scale'][0]
    print(f'DEBUG: shape is {shape} and scale is {scale}!!!')

    if row['n'][0] > 100:
        pCancel = round(100*row['pCancel'][0], 1)
    else:
        pCancel = None

    # Set emoji and color based on on-time percentage
    if onTime > 90:
        emoji = "🚀"  # rocket emoji for excellent performance
        colorOnTime = "#34A853"  # green
    elif onTime > 85:
        emoji = "😐"  # neutral face for decent performance
        colorOnTime = "#FBBC05"  # yellow
    elif onTime > 70:
        emoji = "😢"  # crying face for poor performance
        colorOnTime = "#F29900"  # orange
    else:
        emoji = "🤬"  # angry face for terrible performance
        colorOnTime = "#EA4335"  # red

    # Set color based on percentage of very late flights
    if tooLate < 5:
        colorTooLate = "#34A853"  # green for few late flights
    elif tooLate < 8:
        colorTooLate = "#FBBC05"  # yellow for some late flights
    elif tooLate < 12:
        colorTooLate = "#F29900"  # orange for many late flights
    else:
        colorTooLate = "#EA4335"  # red for excessive late flights

    # Create warning message for risky flights
    warning_message = ""
    if tooLate > 10 or onTime < 75:
        warning_message = f"""
            <div style="margin: 8px 0; padding: 8px; background-color: #2a1810; border-left: 3px solid #ff9800; border-radius: 4px; font-size: 12px;">
                <div><span style="margin-right: 6px;">⚠️</span><span style="color: #ffcc02; font-weight: 500;">This flight is terrible</span></div>
                <div style="margin-top: 4px;">You better be getting a great deal!</div>
            </div>
        """
    
    # Add cancellation data if available with color coding
    cancel_message = ""
    if pCancel is not None:
        if pCancel < 1:
            cancel_color = "#34A853"  # green
        elif pCancel < 1.5:
            cancel_color = "#FBBC05"  # yellow
        elif pCancel < 2:
            cancel_color = "#F29900"  # orange
        else:
            cancel_color = "#EA4335"  # red
            
        cancel_message = f"""
            <div>This flight is historically canceled <span style="color: {cancel_color}; font-weight: bold;">{pCancel}%</span> of the time</div>
        """

    # All inline: first a box labeled "leaves on time" with the onTime percentage
    # then a box labeled ">1h LATE" with the tooLate percentage
    # then a box labeled "FlyOnTime score" with the emoji
    returnme = f"""
    <div id="flyontime-data" style="display: flex; gap: 12px; align-items: center; justify-content: center; height: 100%; font-family: 'Google Sans', Arial, sans-serif; position: relative; cursor: pointer;">
        <div style="text-align: center; min-width: 80px;">
            <div style="font-size: 13px; font-weight: 500; margin-bottom: 3px; color: #5f6368;">Leaves on Time</div>
            <div style="font-size: 22px; font-weight: bold; color: {colorOnTime}; line-height: 1;">{onTime}%</div>
        </div>
        <div style="text-align: center; min-width: 80px;">
            <div style="font-size: 13px; font-weight: 500; margin-bottom: 3px; color: #5f6368;">Leaves >1h Late</div>
            <div style="font-size: 22px; font-weight: bold; color: {colorTooLate}; line-height: 1;">{tooLate}%</div>
        </div>
        <div style="display: flex; align-items: center; justify-content: center; margin-left: 6px;">
            <div style="font-size: 32px; line-height: 1;">{emoji}</div>
        </div>
        <div class="flyontime-tooltip" style="
            display: none;
            position: absolute;
            bottom: 120%;
            left: 50%;
            transform: translateX(-50%);
            background-color: #333;
            color: white;
            padding: 12px;
            border-radius: 8px;
            font-size: 14px;
            min-width: 540px;
            z-index: 1000;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            border: 1px solid #555;">
            <div style="font-weight: bold; margin-bottom: 8px;">✈️ <b>FlyOnTime Analysis</b></div>
            <div>This flight leaves on time* <span style="color: {colorOnTime}; font-weight: bold;">{onTime}%</span> of the time</div>
            <div>And this flight is over an hour late <span style="color: {colorTooLate}; font-weight: bold;">{tooLate}%</span> of the time</div>
            {cancel_message}
            {warning_message}
            <div style="margin-top: 8px;">
                <div style="font-size: 12px; color: #ccc; margin-bottom: 4px;">Delay Distribution</div>
                <svg id="delay-chart" width="520" height="80" data-shape="{shape}" data-scale="{scale}" data-mean="{delayMean}" data-std="{delayStd}" data-tail="{tooLate}"></svg>
            </div>
            <div style="margin-top: 4px; font-size: 12px; color: #ccc;">*leaving within 15mins of scheduled departure time </div>
            <div style="
                position: absolute;
                top: 100%;
                left: 50%;
                transform: translateX(-50%);
                width: 0;
                height: 0;
                border-left: 6px solid transparent;
                border-right: 6px solid transparent;
                border-top: 6px solid #333;">
            </div>
        </div>
    </div>
    """
    return returnme


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)
