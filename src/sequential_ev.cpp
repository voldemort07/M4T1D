#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

using namespace std;

struct StationStats
{
    string suburb;
    double latitude = 0.0;
    double longitude = 0.0;

    long long totalRecords = 0;
    long long totalPorts = 0;
    long long totalOccupied = 0;
    long long totalQueue = 0;
    long long totalSessionMinutes = 0;
};

struct StationResult
{
    string stationId;
    string suburb;

    double avgOccupancy = 0.0;
    double avgQueue = 0.0;
    double avgSessionMinutes = 0.0;
    double estimatedWaitMinutes = 0.0;
    double congestionScore = 0.0;
};

vector<string> splitCSV(const string& line)
{
    vector<string> fields;
    string field;
    stringstream ss(line);

    while (getline(ss, field, ','))
    {
        fields.push_back(field);
    }

    return fields;
}

double calculateCongestionScore(
    double occupancyPercent,
    double avgQueue)
{
    // Occupancy contributes most of the score.
    // Queue length increases the congestion score further.
    double queueComponent = min(avgQueue * 5.0, 20.0);

    return min(
        100.0,
        occupancyPercent * 0.8 + queueComponent
    );
}

double estimateWaitTime(
    double avgQueue,
    double avgSessionMinutes,
    double avgPorts)
{
    if (avgQueue <= 0.0 || avgPorts <= 0.0)
        return 0.0;

    return (avgQueue * avgSessionMinutes) / avgPorts;
}

int main(int argc, char* argv[])
{
    if (argc != 2)
    {
        cout << "Usage: ./sequential_ev <csv_file>\n";
        return 1;
    }

    string filename = argv[1];

    ifstream file(filename);

    if (!file.is_open())
    {
        cerr << "Could not open file: " << filename << "\n";
        return 1;
    }

    auto start = chrono::high_resolution_clock::now();

    map<string, StationStats> stationStats;

    string line;

    // Skip CSV header
    getline(file, line);

    long long recordsProcessed = 0;

    while (getline(file, line))
    {
        if (line.empty())
            continue;

        vector<string> fields = splitCSV(line);

        if (fields.size() != 9)
            continue;

        string stationId = fields[1];
        string suburb = fields[2];

        double latitude = stod(fields[3]);
        double longitude = stod(fields[4]);

        int totalPorts = stoi(fields[5]);
        int occupiedPorts = stoi(fields[6]);
        int queueLength = stoi(fields[7]);
        int sessionMinutes = stoi(fields[8]);

        StationStats& stats = stationStats[stationId];

        stats.suburb = suburb;
        stats.latitude = latitude;
        stats.longitude = longitude;

        stats.totalRecords++;
        stats.totalPorts += totalPorts;
        stats.totalOccupied += occupiedPorts;
        stats.totalQueue += queueLength;
        stats.totalSessionMinutes += sessionMinutes;

        recordsProcessed++;
    }

    file.close();

    vector<StationResult> results;

    for (const auto& entry : stationStats)
    {
        const string& stationId = entry.first;
        const StationStats& stats = entry.second;

        double avgPorts =
            static_cast<double>(stats.totalPorts) /
            stats.totalRecords;

        double avgOccupied =
            static_cast<double>(stats.totalOccupied) /
            stats.totalRecords;

        double avgQueue =
            static_cast<double>(stats.totalQueue) /
            stats.totalRecords;

        double avgSession =
            static_cast<double>(stats.totalSessionMinutes) /
            stats.totalRecords;

        double occupancyPercent =
            (avgOccupied / avgPorts) * 100.0;

        double waitTime = estimateWaitTime(
            avgQueue,
            avgSession,
            avgPorts
        );

        double congestionScore =
            calculateCongestionScore(
                occupancyPercent,
                avgQueue
            );

        results.push_back({
            stationId,
            stats.suburb,
            occupancyPercent,
            avgQueue,
            avgSession,
            waitTime,
            congestionScore
        });
    }

    sort(
        results.begin(),
        results.end(),
        [](const StationResult& a, const StationResult& b)
        {
            return a.congestionScore > b.congestionScore;
        }
    );

    auto end = chrono::high_resolution_clock::now();

    double elapsed =
        chrono::duration<double>(end - start).count();

    cout << fixed << setprecision(2);

    cout << "\nEV Charging Analytics - Sequential\n";
    cout << "---------------------------------\n";

    cout << "Input file: " << filename << "\n";
    cout << "Records processed: "
         << recordsProcessed << "\n";

    cout << "Stations: "
         << results.size() << "\n";

    cout << "Execution time: "
         << elapsed << " seconds\n\n";

    cout << "Station congestion ranking\n\n";

    cout
        << left
        << setw(10) << "Station"
        << setw(18) << "Suburb"
        << setw(14) << "Occupancy"
        << setw(12) << "Avg Queue"
        << setw(16) << "Wait (min)"
        << setw(12) << "Score"
        << "\n";

    cout << string(82, '-') << "\n";

    for (const StationResult& result : results)
    {
        cout
            << left
            << setw(10) << result.stationId
            << setw(18) << result.suburb
            << setw(14)
            << (to_string(
                    static_cast<int>(
                        result.avgOccupancy
                    )
                ) + "%")
            << setw(12) << result.avgQueue
            << setw(16) << result.estimatedWaitMinutes
            << setw(12) << result.congestionScore
            << "\n";
    }

    if (!results.empty())
    {
        const StationResult& busiest = results.front();
        const StationResult& alternative = results.back();

        cout << "\nMost congested station:\n";
        cout
            << busiest.stationId
            << " - "
            << busiest.suburb
            << " | Score: "
            << busiest.congestionScore
            << "\n";

        cout << "\nRecommended lower-congestion alternative:\n";
        cout
            << alternative.stationId
            << " - "
            << alternative.suburb
            << " | Score: "
            << alternative.congestionScore
            << "\n";
    }

    return 0;
}