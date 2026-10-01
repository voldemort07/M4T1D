#include <mpi.h>
#include <omp.h>

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using namespace std;

const int NUM_STATIONS = 10;

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

int stationIndex(const string& stationId)
{
    // EV001 -> 0
    // EV010 -> 9
    return stoi(stationId.substr(2)) - 1;
}

string stationName(int index)
{
    int number = index + 1;

    if (number < 10)
        return "EV00" + to_string(number);

    return "EV0" + to_string(number);
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
        {
            cout
                << "Usage: mpirun -np <processes> "
                << "./hybrid_ev <csv_file>\n";
        }

        MPI_Finalize();
        return 1;
    }

    string filename = argv[1];

    MPI_Barrier(MPI_COMM_WORLD);
    double start = MPI_Wtime();

    ifstream file(filename);

    if (!file.is_open())
    {
        cerr
            << "Rank "
            << rank
            << " could not open file: "
            << filename
            << "\n";

        MPI_Finalize();
        return 1;
    }

    string line;

    // Skip CSV header
    getline(file, line);

    vector<string> localLines;

    long long lineNumber = 0;

    while (getline(file, line))
    {
        if (line.empty())
            continue;

        if (lineNumber % size == rank)
        {
            localLines.push_back(line);
        }

        lineNumber++;
    }

    file.close();

    int maxThreads = omp_get_max_threads();

    // One private station array per OpenMP thread
    vector<vector<StationStats>> threadStats(
        maxThreads,
        vector<StationStats>(NUM_STATIONS)
    );

    #pragma omp parallel
    {
        int threadId = omp_get_thread_num();

        #pragma omp for schedule(static)
        for (long long i = 0;
             i < static_cast<long long>(localLines.size());
             i++)
        {
            vector<string> fields =
                splitCSV(localLines[i]);

            if (fields.size() != 9)
                continue;

            string stationId = fields[1];

            int index = stationIndex(stationId);

            if (index < 0 || index >= NUM_STATIONS)
                continue;

            int totalPorts = stoi(fields[5]);
            int occupiedPorts = stoi(fields[6]);
            int queueLength = stoi(fields[7]);
            int sessionMinutes = stoi(fields[8]);

            StationStats& stats =
                threadStats[threadId][index];

            stats.totalRecords++;
            stats.totalPorts += totalPorts;
            stats.totalOccupied += occupiedPorts;
            stats.totalQueue += queueLength;
            stats.totalSessionMinutes += sessionMinutes;
        }
    }

    StationStats localStats[NUM_STATIONS];

    // Merge OpenMP thread-local results
    for (int t = 0; t < maxThreads; t++)
    {
        for (int i = 0; i < NUM_STATIONS; i++)
        {
            localStats[i].totalRecords +=
                threadStats[t][i].totalRecords;

            localStats[i].totalPorts +=
                threadStats[t][i].totalPorts;

            localStats[i].totalOccupied +=
                threadStats[t][i].totalOccupied;

            localStats[i].totalQueue +=
                threadStats[t][i].totalQueue;

            localStats[i].totalSessionMinutes +=
                threadStats[t][i].totalSessionMinutes;
        }
    }

    long long localRecordsArray[NUM_STATIONS] = {};
    long long localPortsArray[NUM_STATIONS] = {};
    long long localOccupiedArray[NUM_STATIONS] = {};
    long long localQueueArray[NUM_STATIONS] = {};
    long long localSessionArray[NUM_STATIONS] = {};

    long long localRecordsProcessed = 0;

    for (int i = 0; i < NUM_STATIONS; i++)
    {
        localRecordsArray[i] =
            localStats[i].totalRecords;

        localPortsArray[i] =
            localStats[i].totalPorts;

        localOccupiedArray[i] =
            localStats[i].totalOccupied;

        localQueueArray[i] =
            localStats[i].totalQueue;

        localSessionArray[i] =
            localStats[i].totalSessionMinutes;

        localRecordsProcessed +=
            localStats[i].totalRecords;
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
        &localRecordsProcessed,
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

            double avgPorts =
                static_cast<double>(
                    globalPortsArray[i]
                ) /
                globalRecordsArray[i];

            double avgOccupied =
                static_cast<double>(
                    globalOccupiedArray[i]
                ) /
                globalRecordsArray[i];

            double avgQueue =
                static_cast<double>(
                    globalQueueArray[i]
                ) /
                globalRecordsArray[i];

            double avgSession =
                static_cast<double>(
                    globalSessionArray[i]
                ) /
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
                stationName(i),
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
            [](const StationResult& a,
               const StationResult& b)
            {
                return
                    a.congestionScore >
                    b.congestionScore;
            }
        );

        double end = MPI_Wtime();

        cout << fixed << setprecision(2);

        cout
            << "\nEV Charging Analytics - MPI + OpenMP\n";

        cout
            << "------------------------------------\n";

        cout
            << "Input file: "
            << filename
            << "\n";

        cout
            << "MPI processes: "
            << size
            << "\n";

        cout
            << "OpenMP threads per rank: "
            << maxThreads
            << "\n";

        cout
            << "Records processed: "
            << totalRecordsProcessed
            << "\n";

        cout
            << "Execution time: "
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
                << setw(10)
                << result.stationId

                << setw(14)
                << (
                    to_string(
                        static_cast<int>(
                            result.avgOccupancy
                        )
                    ) + "%"
                )

                << setw(12)
                << result.avgQueue

                << setw(16)
                << result.estimatedWaitMinutes

                << setw(12)
                << result.congestionScore

                << "\n";
        }

        if (!results.empty())
        {
            cout
                << "\nMost congested station:\n";

            cout
                << results.front().stationId
                << " | Score: "
                << results.front().congestionScore
                << "\n";

            cout
                << "\nRecommended lower-congestion alternative:\n";

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