/**
 * Describe cases against data to fed into fixtures/scoresLoad.es6 :: install()
 */
 const UNCHANGED_WAIT = 1000;
 // ID_ALIAS must match src/LiveSelect.es6:20
 const ID_ALIAS       = '_id';

exports.data = {  
  assignments: [
    { id:1, class_id:1, name: 'Assignment 1', value:64 },
    { id:2, class_id:1, name: 'Assignment 2', value:29 },
    { id:3, class_id:1, name: 'Assignment 3', value:57 }
  ],
  students: [
    { id:1, name: 'Student 1' },
    { id:2, name: 'Student 2' },
    { id:3, name: 'Student 3' }
  ],
  scores: [
    { id:1, assignment_id:1, student_id:1, score:52 },
    { id:2, assignment_id:1, student_id:2, score:54 },
    { id:3, assignment_id:1, student_id:3, score:28 },
  ]
}

exports.cases = {}
/**
  Each case is described by an object with a query and a set of events
  Each case is provided with a clean data set, as described above
  exports.cases.<case identifier string> = {
    query: <SQL SELECT statement string>,
    events: [
      Each event is described by an object with a single property
      The key can be 'data', 'diff', 'perform', or 'unchanged'
      { data: <deepEqual to most recent update event data> }
      { diff: <deepEqual to most recent update event diff> }
      { perform: [<array of SQL queries>] }
      { unchanged: <milliseconds to wait> }
    ]
  }
 */
exports.cases.innerJoin = {
  query: `
    SELECT
      students.name  AS student_name,
      assignments.name,
      assignments.value,
      scores.score
    FROM
      scores
    INNER JOIN assignments ON
      (assignments.id = scores.assignment_id)
    INNER JOIN students ON
      (students.id = scores.student_id)
    WHERE
      assignments.class_id = 1
  `,
  events: [
    { data: [ 
      { [ID_ALIAS]: 'assignments:1|scores:1|students:1',
        student_name: 'Student 1', name: 'Assignment 1', value: 64, score: 52 },
      { [ID_ALIAS]: 'assignments:1|scores:2|students:2',
        student_name: 'Student 2', name: 'Assignment 1', value: 64, score: 54 },
      { [ID_ALIAS]: 'assignments:1|scores:3|students:3',
        student_name: 'Student 3', name: 'Assignment 1', value: 64, score: 28 }
    ] },
    { perform: [
      `INSERT INTO scores (id, assignment_id, student_id, score) VALUES
        (4, 2, 1, 25)`
    ] },
    { diff: [ [ 'added',
      { [ID_ALIAS]: 'assignments:2|scores:4|students:1',
        student_name: 'Student 1', name: 'Assignment 2', value: 29, score: 25 }
    ] ] },
    { perform: [
      // student_id does not exist, will not be in result set
      `INSERT INTO scores (id, assignment_id, student_id, score) VALUES
        (5, 2, 4, 25)`
    ] },
    { unchanged: UNCHANGED_WAIT },
    { perform: [
      `UPDATE scores SET score = 21 WHERE id = 4`
    ] },
    { diff: [ [ 'changed',
      { [ID_ALIAS]: 'assignments:2|scores:4|students:1',
        student_name: 'Student 1', name: 'Assignment 2', value: 29, score: 25 },
      { [ID_ALIAS]: 'assignments:2|scores:4|students:1',
        student_name: 'Student 1', name: 'Assignment 2', value: 29, score: 21 }
    ] ] },
    { perform: [
      `UPDATE students SET name = 'John Doe' WHERE id = 2`
    ] },
    { diff: [ [ 'changed',
      { [ID_ALIAS]: 'assignments:1|scores:2|students:2',
        student_name: 'Student 2', name: 'Assignment 1', value: 64, score: 54 },
      { [ID_ALIAS]: 'assignments:1|scores:2|students:2',
        student_name: 'John Doe',  name: 'Assignment 1', value: 64, score: 54 }
    ] ] },
    { perform: [
      `DELETE FROM scores WHERE id = 4`
    ] },
    { diff: [ [ 'removed',
      { [ID_ALIAS]: 'assignments:2|scores:4|students:1',
        student_name: 'Student 1', name: 'Assignment 2', value: 29, score: 21 }
    ] ] },
    { perform: [
      // assignment with different class_id, no changes
      `INSERT INTO assignments (id, class_id, name, value) VALUES
        (4, 2, 'Another Class', 20)`,
      `INSERT INTO scores (id, assignment_id, student_id, score) VALUES
        (6, 4, 1, 15)`
    ] },
    { unchanged: UNCHANGED_WAIT },
    { perform: [
      `UPDATE scores SET score = 19 WHERE id = 6`
    ] },
    { unchanged: UNCHANGED_WAIT },
  ]
}

exports.cases.leftRightJoin = {
  query: `
    SELECT
      students.name  AS student_name,
      assignments.name,
      assignments.value,
      scores.score
    FROM
      scores
    RIGHT JOIN assignments ON
      (assignments.id = scores.assignment_id)
    LEFT JOIN students ON
      (students.id = scores.student_id)
    WHERE
      assignments.class_id = 1
  `,
  events: [
    { data: [ 
      { [ID_ALIAS]: 'assignments:1|scores:1|students:1',
        student_name: 'Student 1', name: 'Assignment 1', value: 64, score: 52 },
      { [ID_ALIAS]: 'assignments:1|scores:2|students:2',
        student_name: 'Student 2', name: 'Assignment 1', value: 64, score: 54 },
      { [ID_ALIAS]: 'assignments:1|scores:3|students:3',
        student_name: 'Student 3', name: 'Assignment 1', value: 64, score: 28 },
      { [ID_ALIAS]: 'assignments:2|scores:|students:',
        student_name: null       , name: 'Assignment 2', value: 29, score: null },
      { [ID_ALIAS]: 'assignments:3|scores:|students:',
        student_name: null       , name: 'Assignment 3', value: 57, score: null }
    ] },
    { perform: [
      `INSERT INTO scores (id, assignment_id, student_id, score) VALUES
        (4, 2, 1, 25)`
    ] },
    { diff: [
      [ 'added',
        { [ID_ALIAS]: 'assignments:2|scores:4|students:1',
          student_name: 'Student 1',
          name: 'Assignment 2',
          value: 29,
          score: 25 } ],
      [ 'removed',
        { [ID_ALIAS]: 'assignments:2|scores:|students:',
          student_name: null,
          name: 'Assignment 2',
          value: 29,
          score: null } ]
    ] },
    { perform: [
      `INSERT INTO scores (id, assignment_id, student_id, score) VALUES
        (5, 2, 4, 25)`
    ] },
    { diff: [ [ 'added',
      { [ID_ALIAS]: 'assignments:2|scores:5|students:',
        student_name: null, name: 'Assignment 2', value: 29, score: 25 }
    ] ] },
    { perform: [
      `UPDATE scores SET score = 21 WHERE id = 4`
    ] },
    { diff: [ [ 'changed',
      { [ID_ALIAS]: 'assignments:2|scores:4|students:1',
        student_name: 'Student 1', name: 'Assignment 2', value: 29, score: 25 },
      { [ID_ALIAS]: 'assignments:2|scores:4|students:1',
        student_name: 'Student 1', name: 'Assignment 2', value: 29, score: 21 }
    ] ] },
    { perform: [
      `UPDATE students SET name = 'John Doe' WHERE id = 2`
    ] },
    { diff: [ [ 'changed',
      { [ID_ALIAS]: 'assignments:1|scores:2|students:2',
        student_name: 'Student 2', name: 'Assignment 1', value: 64, score: 54 },
      { [ID_ALIAS]: 'assignments:1|scores:2|students:2',
        student_name: 'John Doe',  name: 'Assignment 1', value: 64, score: 54 }
    ] ] },
    { perform: [
      `DELETE FROM scores WHERE id = 4`
    ] },
    { diff: [ [ 'removed',
      { [ID_ALIAS]: 'assignments:2|scores:4|students:1',
        student_name: 'Student 1', name: 'Assignment 2', value: 29, score: 21 }
    ] ] },
    { perform: [
      // assignment with different class_id, no changes
      `INSERT INTO assignments (id, class_id, name, value) VALUES
        (4, 2, 'Another Class', 20)`,
      `INSERT INTO scores (id, assignment_id, student_id, score) VALUES
        (6, 4, 1, 15)`
    ] },
    { unchanged: UNCHANGED_WAIT },
    { perform: [
      `UPDATE scores SET score = 19 WHERE id = 6`
    ] },
    { unchanged: UNCHANGED_WAIT },
  ]
}

exports.cases.fullJoin = {
  query: `
      SELECT
      assignments.name,
      assignments.value,
      scores.score
    FROM
      scores
    FULL JOIN assignments ON
      (assignments.id = scores.assignment_id)
  `,
  events: [
    { data: [ 
      { [ID_ALIAS]: 'assignments:1|scores:1',
        name: 'Assignment 1', value: 64, score: 52 },
      { [ID_ALIAS]: 'assignments:1|scores:2',
        name: 'Assignment 1', value: 64, score: 54 },
      { [ID_ALIAS]: 'assignments:1|scores:3',
        name: 'Assignment 1', value: 64, score: 28 },
      { [ID_ALIAS]: 'assignments:2|scores:',
        name: 'Assignment 2', value: 29, score: null },
      { [ID_ALIAS]: 'assignments:3|scores:',
        name: 'Assignment 3', value: 57, score: null },
    ] },
    { perform: [
      `INSERT INTO scores (id, assignment_id, student_id, score) VALUES
        (4, 4, 1, 25)`
    ] },
    { diff: [
      [ 'added',
        { [ID_ALIAS]: 'assignments:|scores:4',
          name: null,
          value: null,
          score: 25 } ],
    ] },
  ]
}

// TODO: Aggregate functions to not work
//  https://git.focus-sis.com/beng/pg-notify-trigger/issues/7
var ignore_test_case = {
// exports.cases.max = {
  query: `
    SELECT
      MAX(scores.score)
    FROM
      scores
    INNER JOIN assignments ON
      (assignments.id = scores.assignment_id)
    WHERE
      assignments.class_id = 1
    GROUP BY
      assignments.class_id
  `,
  events: [
    { data: [ 
      { max: 54 },
    ] },
    { perform: [
      `UPDATE scores SET score = 64 WHERE id = 1`
    ] },
    { diff: [
      [ 'changed', { max: 54 }, { max: 64 } ]
    ] },
  ]
}

exports.cases.inExpression = {
  query: `
    SELECT
      scores.score IN (54) as is_54
    FROM
      scores
    INNER JOIN assignments ON
      (assignments.id = scores.assignment_id)
    WHERE
      assignments.class_id = 1
  `,
  events: [
    { data: [ 
      { [ID_ALIAS]: 'assignments:1|scores:1', is_54: false },
      { [ID_ALIAS]: 'assignments:1|scores:2', is_54: true },
      { [ID_ALIAS]: 'assignments:1|scores:3', is_54: false }
    ] },
    { perform: [
      `UPDATE scores SET score = 64 WHERE id = 2`
    ] },
    { diff: [
      [ 'changed',
        { [ID_ALIAS]: 'assignments:1|scores:2', is_54: true },
        { [ID_ALIAS]: 'assignments:1|scores:2', is_54: false } ]
    ] },
  ]
}

exports.cases.allExpression = {
  query: `
    SELECT
      score < ALL (SELECT score FROM scores WHERE score > 28) is_lte_28
    FROM
      scores
    INNER JOIN assignments ON
      (assignments.id = scores.assignment_id)
    WHERE
      assignments.class_id = 1
  `,
  events: [
    { data: [ 
      { [ID_ALIAS]: 'assignments:1|scores:1', is_lte_28: false },
      { [ID_ALIAS]: 'assignments:1|scores:2', is_lte_28: false },
      { [ID_ALIAS]: 'assignments:1|scores:3', is_lte_28: true }
    ] },
    { perform: [
      `UPDATE scores SET score = 14 WHERE id = 2`
    ] },
    { diff: [
      [ 'changed',
        { [ID_ALIAS]: 'assignments:1|scores:2', is_lte_28: false },
        { [ID_ALIAS]: 'assignments:1|scores:2', is_lte_28: true } ]
    ] },
  ]
}


