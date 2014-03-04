/* Here is a list of proposed queries to meet the specifications for
   the BigData project.

Project:	"BigData"
Group No:	Group 3
Class:		CMPUT 391
Term:		Winter 2014
Authors:	Shenwei Liao, Anthony Ou, Jacqueline Terlaan

*/

/* Specification of query requirements. 
   (See also: Section 2.3 of project specification document.)
   
   	1)	4/5 queries will retrieve information from ALL distributed nodes.
   	2)	1 query must contain >= 10 atomic conditions in the WHERE clause.
   	3)	2 queries must contain both of: GROUP BY and ORDER BY clauses.
   	4)	>= 3 queries must be "range" queries: i.e., retrieving all rows
   		where a column value is between an upper and lower boundary.
	5)	0 queries are trivial in nature. (No simple key searches.)
*/

/* Proposed strategies/options:

   	A)	Attack requirement no. 1 first. Then it is possible to reserve
		one of the five queries to meet some other requirement without
		necessarily incurring extra costs. (i.e., it might not be 
		optimal if it were to have to be executed over all nodes in
		the cluser, but it can meet its requirement relatively easily.)

   		Potential difficulties:

			Prob 1)	We may not yet know the nature of how the data will
					be distributed.
			Soln 1)	Use this query to plan that out. When writing queries
					to satisfy requirement no. 1, decide upon whether to
					partition vertically or horizontally, and make all of
					the queries which will meet this requirement "optimal"
					for that type of partition.


   	B)	Attack requirements no. 2, 3 and 4 first.

   		Potential difficulties:

			Prob 1)	We may not be able to find a way to make the queries
					meet requirement no. 1 easily and/or efficiently.
			Soln 1) None so far.

	C)	Save requirement 2 for the query which will not meet requirement 1.
		This could save a lot of time. (Does not necessarily contradict
		strategy A).

		Potential difficulties:

			Prob 1)	Queries which meet requirement 3 could take a long
					time, because they will be distributed over all of
					the nodes in the cluser.
			Soln 1)	Make queries which are to meet requirement 3 very,
					very simple. (Probably not possible, though.).

	D)	Save requirement 2 for one of the two queries which are to meet
		requirement 3 (i.e., they will have both GROUP BY and ORDER BY
		clauses.)

		Potential difficulties:
			Prob 1)	Bloody expensive.
			Soln 1) I dunno m8. 'Ave a giggle.

	E)	Make queries which are to meet requirement 2 relatively simple,
		removing any unnecesary nesting.

			Duh.

	F) 	Make one query which meets requirement 2 also meet requirement 4.
		i.e., kill two birds with one stone. Making a range query with
		TONS of other atomic conditions in the WHERE clause should be
		easy, right?

		Potential difficulties:
		
			Prob 1) That query could become very expensive?
			Soln 1) That query will only have to be executed once, right?
					We could reserve that query as the one which will not
					be distributed over all nodes in the cluster if it is
					absolutely necessary.

*/

/* Queries which satisfy strategies A and F. 							*/

/* Query 1:  */
/* Should meet requirements: 1, 3.	*/
/* Meets requirements: None so far.	*/

/* Query 2:  */
/* Should meet requirements: 1, 3.	*/
/* Meets requirements: None so far.	*/

/* Query 3:  */
/* Should meet requirements: 1, 4.	*/
/* Meets requirements: None so far.	*/

/* Query 4:  */
/* Should meet requirements: 1, 4.	*/
/* Meets requirements: None so far.	*/

/* Query 5:  */
/* Should meet requirements: 2, 4.	*/
/* Meets requirements: None so far.	*/


/* Queries which satisfy other strategies. 								*/
