#include <mpi.h>
#include <algorithm>
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
    long long totalRecords = 0;
    long long totalPorts = 0;
    long long totalOccupied = 0;
    long long totalQueue = 0;
    long long totalSessionMinutes = 0;
};

struct StationResult
{
    string stationId;
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
        fields.push_back(field);

    return fields;
}

double calculateCongestionScore(
    double occupancyPercent,
    double avgQueue)
{
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
    MPI_Init(&argc, &argv);

    int rank, size;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc != 2)
    {
        if (rank == 0)
            cout << "Usage: mpirun -np <processes> ./mpi_ev <csv_file>\n";

        MPI_Finalize();
        return 1;
    }

    string filename = argv[1];

    MPI_Barrier(MPI_COMM_WORLD);
    double start = MPI_Wtime();

    ifstream file(filename);

    if (!file.is_open())
    {
        cerr << "Rank " << rank
             << " could not open file: "
             << filename << "\n";

        MPI_Finalize();
        return 1;
    }

    map<string, StationStats> localStats;

    string line;

    // Skip header
    getline(file, line);

    long long lineNumber = 0;
    long long localRecords = 0;

    while (getline(file, line))
    {
        if (line.empty())
            continue;

        if (lineNumber % size == rank)
        {
            vector<string> fields = splitCSV(line);

            if (fields.size() == 9)
            {
                string stationId = fields[1];

                int totalPorts = stoi(fields[5]);
                int occupiedPorts = stoi(fields[6]);
                int queueLength = stoi(fields[7]);
                int sessionMinutes = stoi(fields[8]);

                StationStats& stats = localStats[stationId];

                stats.totalRecords++;
                stats.totalPorts += totalPorts;
                stats.totalOccupied += occupiedPorts;
                stats.totalQueue += queueLength;
                stats.totalSessionMinutes += sessionMinutes;

                localRecords++;
            }
        }

        lineNumber++;
    }

    file.close();

    const int NUM_STATIONS = 10;

    long long localRecordsArray[NUM_STATIONS] = {};
    long long localPortsArray[NUM_STATIONS] = {};
    long long localOccupiedArray[NUM_STATIONS] = {};
    long long localQueueArray[NUM_STATIONS] = {};
    long long localSessionArray[NUM_STATIONS] = {};

    for (int i = 0; i < NUM_STATIONS; i++)
    {
        string stationId = "EV";

        if (i + 1 < 10)
            stationId += "00";
        else
            stationId += "0";

        stationId += to_string(i + 1);

        if (localStats.count(stationId))
        {
            StationStats& stats = localStats[stationId];

            localRecordsArray[i] = stats.totalRecords;
            localPortsArray[i] = stats.totalPorts;
            localOccupiedArray[i] = stats.totalOccupied;
            localQueueArray[i] = stats.totalQueue;
            localSessionArray[i] = stats.totalSessionMinutes;
        }
    }

    long long globalRecordsArray[NUM_STATIONS] = {};
    long long globalPortsArray[NUM_STATIONS] = {};
    long long globalOccupiedArray[NUM_STATIONS] = {};
    long long globalQueueArray[NUM_STATIONS] = {};
    long long globalSessionArray[NUM_STATIONS] = {};

    MPI_Reduce(
        localRecordsArray,
        globalRecordsArray,
        NUM_STATIONS,
        MPI_LONG_LONG,
        MPI_SUM,
        0,
        MPI_COMM_WORLD
    );

    MPI_Reduce(
        localPortsArray,
        globalPortsArray,
        NUM_STATIONS,
        MPI_LONG_LONG,
        MPI_SUM,
        0,
        MPI_COMM_WORLD
    );

    MPI_Reduce(
        localOccupiedArray,
        globalOccupiedArray,
        NUM_STATIONS,
        MPI_LONG_LONG,
        MPI_SUM,
        0,
        MPI_COMM_WORLD
    );

    MPI_Reduce(
        localQueueArray,
        globalQueueArray,
        NUM_STATIONS,
        MPI_LONG_LONG,
        MPI_SUM,
        0,
        MPI_COMM_WORLD
    );

    MPI_Reduce(
        localSessionArray,
        globalSessionArray,
        NUM_STATIONS,
        MPI_LONG_LONG,
        MPI_SUM,
        0,
        MPI_COMM_WORLD
    );

    long long totalRecordsProcessed = 0;

    MPI_Reduce(
        &localRecords,
        &totalRecordsProcessed,
        1,
        MPI_LONG_LONG,
        MPI_SUM,
        0,
        MPI_COMM_WORLD
    );

    if (rank == 0)
    {
        vector<StationResult> results;

        for (int i = 0; i < NUM_STATIONS; i++)
        {
            if (globalRecordsArray[i] == 0)
                continue;

            string stationId = "EV";

            if (i + 1 < 10)
                stationId += "00";
            else
                stationId += "0";

            stationId += to_string(i + 1);

            double avgPorts =
                static_cast<double>(globalPortsArray[i]) /
                globalRecordsArray[i];

            double avgOccupied =
                static_cast<double>(globalOccupiedArray[i]) /
                globalRecordsArray[i];

            double avgQueue =
                static_cast<double>(globalQueueArray[i]) /
                globalRecordsArray[i];

            double avgSession =
                static_cast<double>(globalSessionArray[i]) /
                globalRecordsArray[i];

            double occupancyPercent =
                (avgOccupied / avgPorts) * 100.0;

            double waitTime =
                estimateWaitTime(
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

        double end = MPI_Wtime();

        cout << fixed << setprecision(2);

        cout << "\nEV Charging Analytics - MPI\n";
        cout << "---------------------------\n";

        cout << "Input file: "
             << filename << "\n";

        cout << "MPI processes: "
             << size << "\n";

        cout << "Records processed: "
             << totalRecordsProcessed << "\n";

        cout << "Execution time: "
             << (end - start)
             << " seconds\n\n";

        cout << "Station congestion ranking\n\n";

        cout
            << left
            << setw(10) << "Station"
            << setw(14) << "Occupancy"
            << setw(12) << "Avg Queue"
            << setw(16) << "Wait (min)"
            << setw(12) << "Score"
            << "\n";

        cout << string(64, '-') << "\n";

        for (const StationResult& result : results)
        {
            cout
                << left
                << setw(10) << result.stationId
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
            cout << "\nMost congested station:\n";
            cout
                << results.front().stationId
                << " | Score: "
                << results.front().congestionScore
                << "\n";

            cout << "\nRecommended lower-congestion alternative:\n";
            cout
                << results.back().stationId
                << " | Score: "
                << results.back().congestionScore
                << "\n";
        }
    }

    MPI_Finalize();

    return 0;
}