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
    double queueComponent =
        min(avgQueue * 5.0, 20.0);

    return min(
        100.0,
        occupancyPercent * 0.8 +
        queueComponent
    );
}

double estimateWaitTime(
    double avgQueue,
    double avgSessionMinutes,
    double avgPorts)
{
    if (avgQueue <= 0.0 || avgPorts <= 0.0)
        return 0.0;

    return
        (avgQueue * avgSessionMinutes) /
        avgPorts;
}

int main(int argc, char* argv[])
{
    MPI_Init(&argc, &argv);

    int rank, size;

    MPI_Comm_rank(
        MPI_COMM_WORLD,
        &rank
    );

    MPI_Comm_size(
        MPI_COMM_WORLD,
        &size
    );

    if (argc != 2)
    {
        if (rank == 0)
        {
            cout
                << "Usage: mpirun -np <processes> "
                << "./partitioned_hybrid_ev <csv_file>\n";
        }

        MPI_Finalize();
        return 1;
    }

    string filename = argv[1];

    MPI_Barrier(MPI_COMM_WORLD);

    double start = MPI_Wtime();

    ifstream file(
        filename,
        ios::binary
    );

    if (!file.is_open())
    {
        cerr
            << "Rank "
            << rank
            << " could not open file\n";

        MPI_Finalize();
        return 1;
    }

    // Determine total file size
    file.seekg(
        0,
        ios::end
    );

    long long fileSize =
        file.tellg();

    long long chunkSize =
        fileSize / size;

    long long startByte =
        rank * chunkSize;

    long long endByte;

    if (rank == size - 1)
        endByte = fileSize;
    else
        endByte =
            (rank + 1) * chunkSize;

    // Non-zero ranks start one byte early so the
    // line they discard always belongs to the
    // previous rank, even if startByte lands
    // exactly on the start of a record.
    file.seekg(
        rank == 0 ? 0 : startByte - 1,
        ios::beg
    );

    string line;

    // Rank 0: skip CSV header.
    // Other ranks: skip the tail of the previous
    // rank's last record.
    getline(file, line);

    vector<string> localLines;

    while (file.good())
    {
        long long currentPosition =
            file.tellg();

        if (currentPosition == -1)
            break;

        if (
            currentPosition >= endByte &&
            rank != size - 1
        )
        {
            break;
        }

        if (!getline(file, line))
            break;

        if (!line.empty())
            localLines.push_back(line);
    }

    file.close();

    int maxThreads =
        omp_get_max_threads();

    vector<vector<StationStats>>
        threadStats(
            maxThreads,
            vector<StationStats>(
                NUM_STATIONS
            )
        );

    #pragma omp parallel
    {
        int threadId =
            omp_get_thread_num();

        #pragma omp for schedule(static)
        for (
            long long i = 0;
            i <
            static_cast<long long>(
                localLines.size()
            );
            i++
        )
        {
            vector<string> fields =
                splitCSV(localLines[i]);

            if (fields.size() != 9)
                continue;

            int index =
                stationIndex(
                    fields[1]
                );

            if (
                index < 0 ||
                index >= NUM_STATIONS
            )
            {
                continue;
            }

            int totalPorts =
                stoi(fields[5]);

            int occupiedPorts =
                stoi(fields[6]);

            int queueLength =
                stoi(fields[7]);

            int sessionMinutes =
                stoi(fields[8]);

            StationStats& stats =
                threadStats
                    [threadId]
                    [index];

            stats.totalRecords++;

            stats.totalPorts +=
                totalPorts;

            stats.totalOccupied +=
                occupiedPorts;

            stats.totalQueue +=
                queueLength;

            stats.totalSessionMinutes +=
                sessionMinutes;
        }
    }

    StationStats
        localStats[NUM_STATIONS];

    for (
        int t = 0;
        t < maxThreads;
        t++
    )
    {
        for (
            int i = 0;
            i < NUM_STATIONS;
            i++
        )
        {
            localStats[i].totalRecords +=
                threadStats[t][i]
                    .totalRecords;

            localStats[i].totalPorts +=
                threadStats[t][i]
                    .totalPorts;

            localStats[i].totalOccupied +=
                threadStats[t][i]
                    .totalOccupied;

            localStats[i].totalQueue +=
                threadStats[t][i]
                    .totalQueue;

            localStats[i]
                .totalSessionMinutes +=
                threadStats[t][i]
                    .totalSessionMinutes;
        }
    }

    long long
        localRecords[NUM_STATIONS] = {};

    long long
        localPorts[NUM_STATIONS] = {};

    long long
        localOccupied[NUM_STATIONS] = {};

    long long
        localQueue[NUM_STATIONS] = {};

    long long
        localSessions[NUM_STATIONS] = {};

    long long localTotal = 0;

    for (
        int i = 0;
        i < NUM_STATIONS;
        i++
    )
    {
        localRecords[i] =
            localStats[i]
                .totalRecords;

        localPorts[i] =
            localStats[i]
                .totalPorts;

        localOccupied[i] =
            localStats[i]
                .totalOccupied;

        localQueue[i] =
            localStats[i]
                .totalQueue;

        localSessions[i] =
            localStats[i]
                .totalSessionMinutes;

        localTotal +=
            localStats[i]
                .totalRecords;
    }

    long long
        globalRecords[NUM_STATIONS] = {};

    long long
        globalPorts[NUM_STATIONS] = {};

    long long
        globalOccupied[NUM_STATIONS] = {};

    long long
        globalQueue[NUM_STATIONS] = {};

    long long
        globalSessions[NUM_STATIONS] = {};

    MPI_Reduce(
        localRecords,
        globalRecords,
        NUM_STATIONS,
        MPI_LONG_LONG,
        MPI_SUM,
        0,
        MPI_COMM_WORLD
    );

    MPI_Reduce(
        localPorts,
        globalPorts,
        NUM_STATIONS,
        MPI_LONG_LONG,
        MPI_SUM,
        0,
        MPI_COMM_WORLD
    );

    MPI_Reduce(
        localOccupied,
        globalOccupied,
        NUM_STATIONS,
        MPI_LONG_LONG,
        MPI_SUM,
        0,
        MPI_COMM_WORLD
    );

    MPI_Reduce(
        localQueue,
        globalQueue,
        NUM_STATIONS,
        MPI_LONG_LONG,
        MPI_SUM,
        0,
        MPI_COMM_WORLD
    );

    MPI_Reduce(
        localSessions,
        globalSessions,
        NUM_STATIONS,
        MPI_LONG_LONG,
        MPI_SUM,
        0,
        MPI_COMM_WORLD
    );

    long long totalRecords = 0;

    MPI_Reduce(
        &localTotal,
        &totalRecords,
        1,
        MPI_LONG_LONG,
        MPI_SUM,
        0,
        MPI_COMM_WORLD
    );

    if (rank == 0)
    {
        vector<StationResult> results;

        for (
            int i = 0;
            i < NUM_STATIONS;
            i++
        )
        {
            if (
                globalRecords[i] == 0
            )
            {
                continue;
            }

            double avgPorts =
                static_cast<double>(
                    globalPorts[i]
                ) /
                globalRecords[i];

            double avgOccupied =
                static_cast<double>(
                    globalOccupied[i]
                ) /
                globalRecords[i];

            double avgQueue =
                static_cast<double>(
                    globalQueue[i]
                ) /
                globalRecords[i];

            double avgSession =
                static_cast<double>(
                    globalSessions[i]
                ) /
                globalRecords[i];

            double occupancy =
                (
                    avgOccupied /
                    avgPorts
                ) * 100.0;

            double wait =
                estimateWaitTime(
                    avgQueue,
                    avgSession,
                    avgPorts
                );

            double score =
                calculateCongestionScore(
                    occupancy,
                    avgQueue
                );

            results.push_back({
                stationName(i),
                occupancy,
                avgQueue,
                avgSession,
                wait,
                score
            });
        }

        sort(
            results.begin(),
            results.end(),
            [](
                const StationResult& a,
                const StationResult& b
            )
            {
                return
                    a.congestionScore >
                    b.congestionScore;
            }
        );

        double end = MPI_Wtime();

        cout << fixed
             << setprecision(2);

        cout
            << "\nEV Charging Analytics "
            << "- Partitioned MPI + OpenMP\n";

        cout
            << "------------------------------------------\n";

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
            << totalRecords
            << "\n";

        cout
            << "Execution time: "
            << (end - start)
            << " seconds\n\n";

        cout
            << "Station congestion ranking\n\n";

        cout
            << left
            << setw(10)
            << "Station"

            << setw(14)
            << "Occupancy"

            << setw(12)
            << "Avg Queue"

            << setw(16)
            << "Wait (min)"

            << setw(12)
            << "Score"

            << "\n";

        cout
            << string(64, '-')
            << "\n";

        for (
            const StationResult&
            result : results
        )
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
                << result
                    .estimatedWaitMinutes

                << setw(12)
                << result
                    .congestionScore

                << "\n";
        }

        if (!results.empty())
        {
            cout
                << "\nMost congested station:\n";

            cout
                << results.front()
                    .stationId

                << " | Score: "

                << results.front()
                    .congestionScore

                << "\n";

            cout
                << "\nRecommended lower-congestion alternative:\n";

            cout
                << results.back()
                    .stationId

                << " | Score: "

                << results.back()
                    .congestionScore

                << "\n";
        }
    }

    MPI_Finalize();

    return 0;
}