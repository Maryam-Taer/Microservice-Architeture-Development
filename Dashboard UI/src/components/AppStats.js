import React, { useEffect, useState } from 'react'
import '../App.css';

export default function AppStats() {
    const [isLoaded, setIsLoaded] = useState(false);
    const [stats, setStats] = useState({});
    const [error, setError] = useState(null)

	const getStats = () => {
	
        fetch(`http://acit3855-setc.eastus.cloudapp.azure.com:8100/stats`) // FIX_ME 
            .then(res => res.json())
            .then((result)=>{
				console.log("Received Stats")
                setStats(result);
                setIsLoaded(true);
            },(error) =>{
                setError(error)
                setIsLoaded(true);
            })
    }
    useEffect(() => {
		const interval = setInterval(() => getStats(), 2000); // Update every 2 seconds
		return() => clearInterval(interval);
    }, [getStats]);

    if (error){
        return (<div className={"error"}>Error found when fetching from API</div>)
    } else if (isLoaded === false){
        return(<div>Loading...</div>)
    } else if (isLoaded === true){
        return( // FIX_ME 
            <div>
                <h1>Latest Stats</h1>
                <table className={"StatsTable"} striped bordered hover>
                    <thead>
                        <tr>
							<th>Find Restaurant</th>
							<th>Write Review</th>
						</tr>
                    </thead>
					<tbody>
						<tr>
							<td># Restaraunt Search Records: {stats['all_rest_records']}</td>
							<td># Posts: {stats['all_review_records']}</td>
						</tr>
						<tr>
							<td colspan="2"># Takeouts available: {stats['num_takeouts_available']}</td>
						</tr>
						<tr>
							<td colspan="2">Top Reviews (i.e. 5/5): {stats['top_reviews']}</td>
						</tr>
					</tbody>
                </table>
                <h3>Last Updated: {stats['last_updated']}</h3>

            </div>
        )
    }
}
